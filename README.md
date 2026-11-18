# Mini Redis (Rust)

직접 구현한 **해시 테이블** 위에 만든 인메모리 키-값 저장소입니다.
HTTP REST API 로 외부에서 사용할 수 있고, TTL 만료 · 패턴 무효화 · 원자적 INCR · AOF/스냅샷 영속성을 지원합니다.

```
┌──────────────┐  HTTP   ┌─────────────────────────── mini-redis-server ───────────────────────────┐
│ curl / 앱 /  │ ──────▶ │  api.rs (axum, tokio 멀티스레드)                                         │
│ demo_api     │ ◀────── │     │  Job(클로저) 를 mpsc 큐로 전송 / oneshot 으로 응답 수신             │
└──────────────┘         │     ▼                                                                    │
                         │  engine.rs  ── 단일 스레드가 Store 를 독점 소유 (락 없음)               │
                         │     │   ├─ 100ms 마다 active expiration                                  │
                         │     │   └─ 1초마다 fsync, AOF 가 커지면 스냅샷으로 압축                  │
                         │     ▼                                                                    │
                         │  store.rs (TTL / NX·XX / INCR / 패턴 삭제)                               │
                         │     ▼                                                                    │
                         │  hashtable.rs (직접 구현: FNV-1a + separate chaining + 자동 리사이즈)    │
                         │                                                                          │
                         │  persistence.rs → data/appendonly.aof, data/dump.jsonl                   │
                         └──────────────────────────────────────────────────────────────────────────┘
```

## 빠른 시작

```bash
# 서버 (기본 127.0.0.1:6380, ./data 에 영속화)
cargo run --release --bin mini-redis-server

# 사용
curl -X PUT localhost:6380/keys/user:1 -H 'content-type: application/json' -d '{"value":"alice","ex":60}'
curl localhost:6380/keys/user:1
# {"key":"user:1","value":"alice","ttl_ms":59874}
curl -X DELETE "localhost:6380/keys?pattern=user:*"
# {"deleted":1}
```

옵션: `--port`, `--host`, `--data-dir`, `--fsync always|everysec|no`, `--no-persist`, `--aof-rewrite-mb`

## API

| Method | Path | Body / Query | 성공 | 실패 |
|---|---|---|---|---|
| GET | `/health` | | 200 | 503 |
| GET | `/stats` | | 200 통계 | |
| GET | `/keys/{key}` | | 200 `{key,value,ttl_ms}` | 404 |
| PUT | `/keys/{key}` | `{"value":"..","ex":초 \| "px":ms, "nx":bool, "xx":bool}` | 200 | 400, 409(nx/xx 불충족), 413 |
| DELETE | `/keys/{key}` | | 200 `{deleted:1}` | 404 |
| GET | `/keys/{key}/ttl` | | 200 `{ttl_ms}` (영구면 null) | 404 |
| PUT | `/keys/{key}/expire` | `{"ex":10}` / `{"px":500}` | 200 | 400, 404 |
| DELETE | `/keys/{key}/expire` | | 200 `{persisted}` | 404 |
| POST | `/keys/{key}/incr` | `{"by":5}` (생략 시 1) | 200 `{value}` | 409 (정수 아님/overflow) |
| GET | `/keys` | `?pattern=user:*&limit=100` | 200 `{keys,count,truncated}` | |
| DELETE | `/keys` | `?pattern=user:*` (**필수**) | 200 `{deleted:n}` | 400 |
| POST | `/admin/flush` | | 200 `{deleted:n}` | |
| POST | `/admin/save` | | 200 `{saved_keys}` | 400 (영속성 꺼짐) |

* 에러는 항상 `{"error":"<code>","message":"..."}` JSON.
* 키에 `/` · 공백 · 한글 등이 있으면 URL 인코딩 (`a/b` → `a%2Fb`). 키 ≤ 1KB, 값 ≤ 1MB, TTL ≤ 100년.
* Rust 에서는 [`MiniRedisClient`](src/client.rs) 로 바로 사용할 수 있습니다.

---

## 1. 해시 테이블 설계 ([src/hashtable.rs](src/hashtable.rs))

| 항목 | 선택 | 이유 |
|---|---|---|
| 해시 함수 | **FNV-1a 64bit** + murmur3 `fmix64` finalizer | 바이트마다 `h ^= b; h *= PRIME` 만 하는 단순·고속 해시. 인덱스로 하위 비트만 쓰므로 finalizer 로 상위 비트를 섞어 분포 보강 |
| 충돌 해결 | **Separate chaining** (`Vec<Vec<Entry>>`) | 구현·설명이 쉽고 삭제가 간단(tombstone 불필요). 체인을 연결 리스트 대신 `Vec` 으로 두어 캐시 친화적 |
| 버킷 수 | 항상 **2의 거듭제곱** | `hash % cap` 대신 `hash & (cap-1)` 비트 연산 |
| 확장 | load factor > **0.75** 이면 2배 | 평균 체인 길이를 1 근처로 유지 → 평균 O(1) |
| 축소 | load factor < **0.125** 이면 절반 | 대량 삭제 후 메모리 반환. 확장/축소 임계값 간격을 넓혀 경계에서 리사이즈가 반복(thrashing)되지 않음 |
| 해시 캐싱 | Entry 에 `hash` 저장 | 리사이즈 시 재해시 불필요, 조회 시 해시가 다르면 문자열 비교 생략 |

주요 함수 동작

* `insert(k, v)`: 해시 → 버킷 → 체인에서 같은 키 찾으면 값 교체(이전 값 반환) / 없으면 push 후 `len+1` → load factor 검사 후 필요 시 `resize(cap*2)`
* `get(k)`: 해시 → 버킷 → 체인 선형 탐색 (`hash` 비교 후 키 비교)
* `remove(k)`: 위치를 찾아 `swap_remove` (버킷 내 순서는 의미 없으므로 O(1)) → 필요 시 축소
* `resize(n)`: 새 버킷 배열을 만들고 캐시된 해시로 각 엔트리를 새 인덱스에 재배치. 비용 O(n) 이지만 2배씩 커지므로 **분할 상환(amortized) O(1)**
* `retain(f)`: 패턴 무효화처럼 조건부 일괄 삭제

> 검증: 강제 충돌 해셔(모든 키가 한 버킷)로 정확성 테스트, `std::HashMap` 과 10만 번 무작위 연산 결과 비교(모델 기반 테스트), 5만 키에서 최장 체인 길이 검사.

## 2. 동시성 설계 ([src/engine.rs](src/engine.rs))

**선택: 단일 스레드 Actor (실제 Redis 와 같은 구조)**

* `Store` 는 엔진 스레드 하나만 소유합니다. HTTP 핸들러(여러 tokio 워커)는 "할 일"을 클로저로 포장해 **mpsc 큐**에 넣고, **oneshot** 채널로 결과를 받습니다.
* 공유 가변 상태가 없으니 **락이 없고 데이터 경쟁이 원천적으로 불가능**합니다. Rust 컴파일러가 `Store` 를 여러 스레드에서 동시에 만질 수 없음을 보장합니다.
* 큐에서 꺼낸 명령은 끝까지 실행된 뒤 다음 명령이 실행되므로 **모든 명령이 원자적**입니다. `INCR` 같은 read-modify-write, `SET NX` 같은 check-then-act 도 경쟁 조건이 없습니다.
* AOF 기록 순서 = 실제 실행 순서 → 복구 결과가 항상 일관됩니다.
* 네트워크 I/O, JSON 직렬화는 tokio 워커들이 병렬 처리하고 엔진은 메모리 연산만 하므로 단일 스레드여도 병목이 되지 않습니다.

| 대안 | 장점 | 단점 |
|---|---|---|
| 전역 `Mutex<HashTable>` | 구현 단순 | 모든 요청이 락 경합, 락 잡은 채 디스크 I/O 시 전체 정지 |
| 샤드별 `RwLock` (예: 16개) | 멀티코어 활용 | 여러 키에 걸친 연산(패턴 삭제, FLUSH, 스냅샷)의 일관성 · AOF 순서 보장이 어려움 |
| **단일 스레드 Actor** ✅ | 락 없음, 명령 원자성, 로그 순서 = 실행 순서 | 한 코어만 사용, 오래 걸리는 명령(KEYS, 스냅샷)은 다른 명령을 지연시킴 |

> 검증: 100개 태스크 × 100회 동시 INCR → 정확히 10,000 / 200개 동시 `SET NX` → 승자 정확히 1명 / HTTP 경유 50×20 동시 INCR → 1,000.

## 3. 만료된 값 처리 ([src/store.rs](src/store.rs))

Redis 와 같은 **2단계 전략**:

1. **Lazy expiration** — 모든 명령은 대상 키를 건드리기 전에 `expire_at <= now` 인지 확인하고, 만료됐으면 그 자리에서 삭제 후 "없는 키"로 처리합니다. → **만료된 값은 절대 응답으로 나가지 않습니다** (GET → 404, NX 는 성공, INCR 은 0 부터 시작).
2. **Active expiration** — 다시 조회되지 않는 만료 키가 메모리를 차지하지 않도록 엔진이 100ms 마다 정리합니다.
   * `(만료시각, 키)` **min-heap** 에서 시간이 지난 것만 꺼내므로 전체 스캔이 필요 없음
   * 한 번에 최대 2,000개만 처리(budget)해 엔진을 오래 멈추지 않음
   * 키를 덮어쓰거나 TTL 을 바꿔도 힙에서 바로 빼지 않고(stale 항목), 꺼낼 때 테이블의 실제 만료 시각과 비교해 다르면 무시
   * stale 항목이 키 수의 2배를 넘으면 힙을 재구성 → 같은 키에 TTL 을 계속 갱신해도 메모리가 무한히 늘지 않음

만료 시각은 **절대 시각(Unix ms)** 으로 저장해 재시작 후에도 TTL 이 이어집니다.
`Store` 는 현재 시각을 인자로 받는 순수 함수 구조라 테스트에서 `sleep` 없이 시간을 조작해 검증합니다.

## 4. 데이터 무효화 방식

| 방식 | API | 용도 |
|---|---|---|
| 명시적 삭제 | `DELETE /keys/{key}` | 원본 데이터가 바뀌었을 때 해당 캐시 즉시 제거 |
| 시간 기반 (TTL) | `PUT` 의 `ex`/`px`, `PUT /expire` | 일정 시간 후 자동 무효화. 삭제를 놓쳐도 **최대 불일치 시간을 TTL 로 제한** |
| TTL 변경/해제 | `PUT /expire`, `DELETE /expire` | 세션 연장, 영구 보관 전환 |
| **패턴 무효화** | `DELETE /keys?pattern=product:42:*` | 연관된 여러 캐시(목록, 상세, 페이지별)를 한 번에 무효화. glob(`*`,`?`,`[a-z]`) 지원 |
| 조건부 저장 | `nx` / `xx` | 이미 있는 값 덮어쓰기 방지, 존재하는 값만 갱신 |
| 전체 삭제 | `POST /admin/flush` | 배포/스키마 변경 시 |

실수 방지: `DELETE /keys` 는 `pattern` 이 없으면 400 (전체 삭제는 별도 엔드포인트).
데모 API 는 **write-invalidate** 패턴을 사용합니다: DB 갱신 **후** `demo:product:{id}` 삭제 + `demo:products:*` 패턴 삭제.

## 5. 서버 다운 대비 영속성 ([src/persistence.rs](src/persistence.rs))

* **AOF (Append-Only File)**: 상태를 바꾸는 명령마다 JSON 한 줄을 파일 끝에 `write`.
  * 프로세스 크래시(`kill -9`): 데이터가 이미 OS 페이지 캐시에 있으므로 **유실 없음**
  * 정전/OS 크래시: fsync 정책으로 조절 — `always`(매 명령, 가장 안전) / `everysec`(기본, 최대 1초 유실) / `no`
* **스냅샷**: 임시 파일에 전체 상태 기록 → `fsync` → `rename` (원자적 교체) → AOF 비움.
  * AOF 가 64MB(설정 가능)를 넘으면 자동 압축, 정상 종료(Ctrl+C/SIGTERM) 시 저장, `POST /admin/save` 로 수동 저장
* **복구 순서**: 스냅샷 로드 → AOF 재실행. 이미 만료된 키는 건너뜀.
* **장애 시나리오 처리**
  * 쓰다가 죽어 마지막 줄이 잘림(개행 없음) → 잘린 부분만 제거하고 기동
  * 중간 줄이 손상 → 데이터 손실을 숨기지 않도록 **기동 거부** (몇 번째 줄인지 알려줌)
  * 스냅샷 rename 후 AOF 를 비우기 전에 죽음 → AOF 의 모든 레코드가 절대값 연산(INCR 도 "결과값 SET"으로 기록)이라 **멱등**: 스냅샷 위에 다시 적용해도 결과 동일

## 6. 데모: API 응답 캐싱 ([src/demo.rs](src/demo.rs))

느린 상품 DB(지연 + 커넥션 풀 제한)를 가진 API 가 Mini Redis 를 **cache-aside** 로 사용합니다.

```bash
cargo run --release --bin mini-redis-server -- --no-persist
cargo run --release --bin demo_api -- --redis-url http://127.0.0.1:6380
curl -i localhost:3000/products/1               # x-cache: MISS (~50ms)
curl -i localhost:3000/products/1               # x-cache: HIT  (~1ms)
curl -i "localhost:3000/products/1?cache=false" # x-cache: BYPASS
curl -i -X PUT localhost:3000/products/1 -H 'content-type: application/json' -d '{"price":500}'
```

고려한 엣지 케이스

| 상황 | 처리 |
|---|---|
| Mini Redis 서버 다운 | 타임아웃 후 DB 로 우회해 정상 응답 (`x-cache: ERROR`) — 캐시는 선택 사항이어야 함 |
| 없는 상품 반복 조회 (cache penetration) | `null` 을 짧은 TTL(5초)로 캐시 |
| 인기 키 동시 MISS (cache stampede) | 키별 single-flight 락 + double-check → DB 조회 1회 |
| 캐시 값 손상 | 역직렬화 실패 시 키 삭제 후 MISS 처리 |
| 데이터 변경 | DB 갱신 후 관련 키 삭제, TTL 로 최대 불일치 시간 제한 |

## 7. 테스트

```bash
cargo test
```

| 종류 | 위치 | 내용 |
|---|---|---|
| 단위 | `src/hashtable.rs` | 삽입/조회/삭제/덮어쓰기, 확장·축소, 강제 충돌, 분포, std HashMap 과 비교 |
| 단위 | `src/store.rs` | lazy/active 만료, stale 힙 항목, NX/XX, TTL/PERSIST, INCR 오류, 패턴 삭제, 힙 크기 제한 |
| 단위 | `src/glob.rs`, `src/api.rs` | 패턴 매칭, TTL/키 검증 |
| 단위 | `src/engine.rs` | 동시 INCR 원자성, 동시 SETNX 승자 1명, 백그라운드 만료, 종료 후 거부 |
| 기능 | `tests/api_test.rs` | 실제 HTTP 서버 대상: 저장/조회/삭제, 만료, 특수문자 키, 큰 값, 잘못된 요청 → JSON 400, 동시성, 503 |
| 기능 | `tests/demo_test.rs` | API 응답 캐싱 HIT/MISS, 무효화, 부정 캐시, 캐시 서버 다운, stampede, 속도 비교 |
| 기능 | `tests/persistence_test.rs` | 정상 재시작, 크래시 복구, 잘린 로그, 손상 로그, 멱등 재실행, AOF 압축 |

## 8. 성능 비교

```bash
cargo run --release --bin bench
# 옵션: --requests 3000 --concurrency 50 --db-latency-ms 20 --db-pool 10 --products 100
```

<!-- BENCH_RESULTS -->
(벤치마크 결과는 실행 후 채워집니다)

## 한계와 개선 아이디어

* 단일 엔진 스레드라 `KEYS`/패턴 삭제/스냅샷처럼 O(n) 작업 중에는 다른 명령이 대기합니다. Redis 는 `SCAN`(커서 기반 점진 순회)과 `fork()` 기반 백그라운드 스냅샷으로 해결합니다.
* 리사이즈가 한 번에 전체를 rehash 합니다. Redis 는 두 테이블을 두고 명령마다 조금씩 옮기는 **점진적 rehash** 로 지연 스파이크를 없앱니다.
* FNV-1a 는 키를 조작한 해시 충돌 공격(HashDoS)에 약합니다. 외부에 공개한다면 SipHash 처럼 시드가 있는 해시를 쓰는 편이 안전합니다 (`HashTable` 은 `BuildHasher` 를 바꿔 끼울 수 있게 만들어 둠).
* 메모리 상한(maxmemory)과 LRU/LFU 축출 정책은 없습니다.
* 같은 데이터 디렉터리를 두 프로세스가 동시에 열지 않도록 막는 파일 락은 없습니다.
