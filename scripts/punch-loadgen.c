#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <stdatomic.h>

enum { HIST_N = 500, HIST_US = 200, MAX_PORTS = 32, MAX_THREADS = 1024 };

static atomic_ullong g_bytes;
static atomic_ullong g_reqs;
static atomic_ullong g_errs;
static atomic_ullong g_lat_max_us;
static atomic_ullong g_hist[HIST_N];
static atomic_int g_go;
static uint64_t g_deadline;
static char *g_body;
static size_t g_size;
static int g_post;

static uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}

static void note_lat(uint64_t us) {
    unsigned i = (unsigned)(us / HIST_US);
    if (i >= HIST_N) {
        i = HIST_N - 1;
    }
    atomic_fetch_add_explicit(&g_hist[i], 1, memory_order_relaxed);
    uint64_t cur = atomic_load_explicit(&g_lat_max_us, memory_order_relaxed);
    while (us > cur && !atomic_compare_exchange_weak_explicit(
                           &g_lat_max_us, &cur, us, memory_order_relaxed, memory_order_relaxed)) {
    }
}

static uint64_t percentile(double p) {
    uint64_t total = 0;
    for (int i = 0; i < HIST_N; i++) {
        total += atomic_load_explicit(&g_hist[i], memory_order_relaxed);
    }
    if (total == 0) {
        return 0;
    }
    uint64_t need = (uint64_t)(total * p + 0.999);
    if (need < 1) {
        need = 1;
    }
    uint64_t cum = 0;
    for (int i = 0; i < HIST_N; i++) {
        cum += atomic_load_explicit(&g_hist[i], memory_order_relaxed);
        if (cum >= need) {
            if (i == HIST_N - 1) {
                return atomic_load_explicit(&g_lat_max_us, memory_order_relaxed);
            }
            return (uint64_t)(i + 1) * HIST_US;
        }
    }
    return atomic_load_explicit(&g_lat_max_us, memory_order_relaxed);
}

static int dial(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int one = 1;
    int buf = 4 * 1024 * 1024;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int write_full(int fd, const char *p, size_t n) {
    while (n) {
        ssize_t w = send(fd, p, n, MSG_NOSIGNAL);
        if (w <= 0) {
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

typedef struct {
    char buf[8192];
    size_t len;
    size_t pos;
} Rbuf;

static void compact(Rbuf *b) {
    if (b->pos == 0) {
        return;
    }
    if (b->pos >= b->len) {
        b->pos = 0;
        b->len = 0;
        return;
    }
    memmove(b->buf, b->buf + b->pos, b->len - b->pos);
    b->len -= b->pos;
    b->pos = 0;
}

static int read_headers(int fd, Rbuf *b, int *status, size_t *clen) {
    compact(b);
    for (;;) {
        char *end = NULL;
        if (b->len >= b->pos + 4) {
            for (size_t i = b->pos; i + 3 < b->len; i++) {
                if (b->buf[i] == '\r' && b->buf[i + 1] == '\n' && b->buf[i + 2] == '\r' &&
                    b->buf[i + 3] == '\n') {
                    end = b->buf + i;
                    break;
                }
            }
        }
        if (end) {
            char save = *end;
            *end = 0;
            char *s = b->buf + b->pos;
            int ok = (strncmp(s, "HTTP/1.1 ", 9) == 0 || strncmp(s, "HTTP/1.0 ", 9) == 0);
            *status = ok ? atoi(s + 9) : 0;
            char *cl = ok ? strcasestr(s, "Content-Length:") : NULL;
            *clen = cl ? strtoul(cl + 15, NULL, 10) : 0;
            *end = save;
            b->pos = (size_t)(end + 4 - b->buf);
            if (!ok || !cl) {
                return -1;
            }
            return 0;
        }
        if (b->len >= sizeof(b->buf) - 1) {
            return -1;
        }
        compact(b);
        ssize_t r = recv(fd, b->buf + b->len, sizeof(b->buf) - b->len, 0);
        if (r <= 0) {
            return -1;
        }
        b->len += (size_t)r;
    }
}

static int discard(int fd, Rbuf *b, size_t n, char *scratch, size_t scratch_n) {
    while (n) {
        if (b->pos < b->len) {
            size_t have = b->len - b->pos;
            if (have > n) {
                have = n;
            }
            b->pos += have;
            n -= have;
            continue;
        }
        b->pos = 0;
        b->len = 0;
        size_t want = n < scratch_n ? n : scratch_n;
        ssize_t r = recv(fd, scratch, want, 0);
        if (r <= 0) {
            return -1;
        }
        n -= (size_t)r;
    }
    return 0;
}

typedef struct {
    char ip[64];
    int port;
} Job;

static void *worker(void *arg) {
    Job *job = arg;
    char req[256];
    int req_len;
    if (g_post) {
        req_len = snprintf(req, sizeof(req),
                           "POST /bench HTTP/1.1\r\nHost: bench\r\nContent-Length: %zu\r\n"
                           "Connection: keep-alive\r\n\r\n",
                           g_size);
    } else {
        req_len = snprintf(req, sizeof(req),
                           "GET /bench?size=%zu HTTP/1.1\r\nHost: bench\r\n"
                           "Connection: keep-alive\r\n\r\n",
                           g_size);
    }
    char *scratch = malloc(256 * 1024);
    if (!scratch || req_len <= 0 || (size_t)req_len >= sizeof(req)) {
        atomic_fetch_add_explicit(&g_errs, 1, memory_order_relaxed);
        free(scratch);
        return NULL;
    }
    while (!atomic_load_explicit(&g_go, memory_order_acquire)) {
    }
    int fd = -1;
    Rbuf rb;
    memset(&rb, 0, sizeof(rb));
    while (now_us() < g_deadline) {
        if (fd < 0) {
            fd = dial(job->ip, job->port);
            memset(&rb, 0, sizeof(rb));
            if (fd < 0) {
                atomic_fetch_add_explicit(&g_errs, 1, memory_order_relaxed);
                usleep(1000);
                continue;
            }
        }
        uint64_t t0 = now_us();
        int bad = write_full(fd, req, (size_t)req_len) != 0;
        if (!bad && g_post) {
            bad = write_full(fd, g_body, g_size) != 0;
        }
        int status = 0;
        size_t clen = 0;
        if (!bad) {
            bad = read_headers(fd, &rb, &status, &clen) != 0;
        }
        if (!bad && g_post && clen < 64) {
            char got[64];
            size_t have = 0;
            if (rb.pos < rb.len) {
                size_t n = rb.len - rb.pos;
                if (n > clen) {
                    n = clen;
                }
                memcpy(got, rb.buf + rb.pos, n);
                rb.pos += n;
                have = n;
            }
            while (have < clen) {
                ssize_t r = recv(fd, got + have, clen - have, 0);
                if (r <= 0) {
                    bad = 1;
                    break;
                }
                have += (size_t)r;
            }
            if (!bad) {
                char expect[64];
                int n = snprintf(expect, sizeof(expect), "{\"bytes\":%zu}\n", g_size);
                if (n <= 0 || (size_t)n != clen || memcmp(got, expect, clen) != 0) {
                    bad = 1;
                }
            }
        } else if (!bad) {
            bad = discard(fd, &rb, clen, scratch, 256 * 1024) != 0;
        }
        if (!bad && (status != 200 || (!g_post && clen != g_size))) {
            bad = 1;
        }
        if (bad) {
            atomic_fetch_add_explicit(&g_errs, 1, memory_order_relaxed);
            close(fd);
            fd = -1;
            continue;
        }
        uint64_t dt = now_us() - t0;
        note_lat(dt);
        atomic_fetch_add_explicit(&g_reqs, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&g_bytes, g_post ? g_size : clen, memory_order_relaxed);
    }
    if (fd >= 0) {
        close(fd);
    }
    free(scratch);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 7) {
        fprintf(stderr, "usage: loadgen IP PORTS GET|POST SIZE SECONDS CONNS_PER_PORT\n");
        return 2;
    }
    signal(SIGPIPE, SIG_IGN);
    const char *ip = argv[1];
    int ports[MAX_PORTS];
    int nports = 0;
    char *list = strdup(argv[2]);
    for (char *tok = strtok(list, ","); tok && nports < MAX_PORTS; tok = strtok(NULL, ",")) {
        ports[nports++] = atoi(tok);
    }
    free(list);
    g_post = strcmp(argv[3], "POST") == 0;
    g_size = (size_t)strtoull(argv[4], NULL, 10);
    unsigned seconds = (unsigned)atoi(argv[5]);
    int conns = atoi(argv[6]);
    if (nports < 1 || g_size == 0 || g_size > 8u * 1024u * 1024u || seconds == 0 || conns < 1) {
        fprintf(stderr, "bad args\n");
        return 2;
    }
    int nthreads = nports * conns;
    if (nthreads > MAX_THREADS) {
        fprintf(stderr, "too many threads\n");
        return 2;
    }
    if (g_post) {
        g_body = malloc(g_size);
        if (!g_body) {
            return 1;
        }
        memset(g_body, 0x5A, g_size);
    }
    Job *jobs = calloc((size_t)nthreads, sizeof(Job));
    pthread_t *ths = calloc((size_t)nthreads, sizeof(pthread_t));
    int idx = 0;
    for (int p = 0; p < nports; p++) {
        for (int c = 0; c < conns; c++) {
            snprintf(jobs[idx].ip, sizeof(jobs[idx].ip), "%s", ip);
            jobs[idx].port = ports[p];
            if (pthread_create(&ths[idx], NULL, worker, &jobs[idx]) != 0) {
                fprintf(stderr, "pthread_create failed\n");
                return 1;
            }
            idx++;
        }
    }
    g_deadline = now_us() + (uint64_t)seconds * 1000000ull;
    uint64_t t0 = now_us();
    atomic_store_explicit(&g_go, 1, memory_order_release);
    for (int i = 0; i < nthreads; i++) {
        pthread_join(ths[i], NULL);
    }
    double elapsed = (double)(now_us() - t0) / 1e6;
    uint64_t bytes = atomic_load(&g_bytes);
    uint64_t reqs = atomic_load(&g_reqs);
    uint64_t errs = atomic_load(&g_errs);
    double mib_s = elapsed > 0 ? (double)bytes / elapsed / (1024.0 * 1024.0) : 0;
    double rps = elapsed > 0 ? (double)reqs / elapsed : 0;
    printf("ports=%d conns_per_port=%d threads=%d method=%s size=%zu seconds=%u\n", nports, conns,
           nthreads, g_post ? "POST" : "GET", g_size, seconds);
    printf("elapsed_s=%.3f bytes=%llu reqs=%llu errors=%llu\n", elapsed,
           (unsigned long long)bytes, (unsigned long long)reqs, (unsigned long long)errs);
    printf("mib_s=%.2f gbps=%.3f rps=%.1f\n", mib_s, mib_s * 8.0 / 1024.0, rps);
    printf("p50_us=%llu p99_us=%llu max_us=%llu\n", (unsigned long long)percentile(0.50),
           (unsigned long long)percentile(0.99),
           (unsigned long long)atomic_load(&g_lat_max_us));
    free(jobs);
    free(ths);
    free(g_body);
    return 0;
}
