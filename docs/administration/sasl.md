# SASL 인증과 접근 제어

## Concept

### ACL Group

ARCUS는 사용자 계정에 대해 인증과 ARCUS 명령의 사용 권한에 따라 ARCUS 접근 및 사용을 제어한다.

이러한 사용자 계정의 그룹을 ACL(Access Control List) 그룹이라 지칭하며, 아래 특성을 가진다.
- 하나의 ACL group에는 여러 사용자가 포함될 수 있으며, 각 사용자에게 권한을 부여할 수 있다.
- 서로 다른 ACL group에는 같은 이름을 갖는 사용자가 존재할 수 있으며, 독립적인 password와 권한을 갖게 된다.
- ARCUS Cache Server는 구동 시 하나의 ACL group을 선택하여 해당 ACL group의 인증/권한 정보에 따라 동작하게 된다.

### SASL 인증 방식

- 현재 `SCRAM-SHA-256` 인증 방식만을 지원한다.
- ZooKeeper에 암호화된 형태의 비밀번호를 보관하며, ZooKeeper에 저장된 데이터를 확인하더라도 해당 계정으로 인증할 수 없다.
- Cyrus SASL 라이브러리 사용하며, ZooKeeper에 인증/권한 정보를 저장하기 위한 arcus auxprop plugin을 직접 구현한다.

### 권한 부여

각 사용자 계정에 부여할 수 있는 권한들은 다음과 같다.
- 특정 type의 item을 조회/수정할 수 있는 권한: `kv`, `list`, `set`, `map`, `btree`
- Item attribute 관련 명령의 수행 권한: `attr`
- Flush 계열 명령의 수행 권한: `flush`
- Scan 명령의 수행 권한: `scan`
- 설정/운영 관련 명령의 수행 권한: `admin`

ARCUS 서버의 ASCII 기반 명령을 수행할 수 있는 권한을 정리하면 아래와 같다.

| Commands | Description | Permission |
| - | - | - |
| [`set`](../ascii-protocol/ch04-command-key-value.md#storage), [`add`](../ascii-protocol/ch04-command-key-value.md#storage), [`replace`](../ascii-protocol/ch04-command-key-value.md#storage), [`append`](../ascii-protocol/ch04-command-key-value.md#storage), [`prepend`](../ascii-protocol/ch04-command-key-value.md#storage), [`cas`](../ascii-protocol/ch04-command-key-value.md#storage), <br> [`get`](../ascii-protocol/ch04-command-key-value.md#retrieval), [`gets`](../ascii-protocol/ch04-command-key-value.md#retrieval), [`mget`](../ascii-protocol/ch04-command-key-value.md#retrieval), [`mgets`](../ascii-protocol/ch04-command-key-value.md#retrieval),  [`incr`](../ascii-protocol/ch04-command-key-value.md#increment-decrement), [`decr`](../ascii-protocol/ch04-command-key-value.md#increment-decrement), [`delete`](../ascii-protocol/ch04-command-key-value.md#deletion) | Key-Value 데이터 조작 명령 | `kv` |
| [`lop`](../ascii-protocol/ch05-command-list-collection.md), [`delete`](../ascii-protocol/ch04-command-key-value.md#deletion) | List Collection 데이터 조작 명령 | `list` *(delete 포함)* |
| [`sop`](../ascii-protocol/ch06-command-set-collection.md), [`delete`](../ascii-protocol/ch04-command-key-value.md#deletion) | Set Collection 데이터 조작 명령 | `set` *(delete 포함)* |
| [`mop`](../ascii-protocol/ch07-command-map-collection.md), [`delete`](../ascii-protocol/ch04-command-key-value.md#deletion) | Map Collection 데이터 조작 명령 | `map` *(delete 포함)* |
| [`bop`](../ascii-protocol/ch08-command-btree-collection.md), [`delete`](../ascii-protocol/ch04-command-key-value.md#deletion) | B+Tree Collection 데이터 조작 명령 | `btree` *(delete 포함)* |
| [`getattr`](../ascii-protocol/ch10-command-item-attribute.md#getattr), [`setattr`](../ascii-protocol/ch10-command-item-attribute.md#setattr), [`touch`](../ascii-protocol/ch04-command-key-value.md#touch) | Item 속성 조회 및 변경 명령 | `attr` |
| [`scan`](../ascii-protocol/ch11-command-scan.md) | 아이템 탐색 명령 | `scan` |
| [`flush_all`](../ascii-protocol/ch13-command-administration.md#flush), [`flush_prefix`](../ascii-protocol/ch13-command-administration.md#flush) | 전체 또는 특정 prefix 삭제 명령 | `flush` |
| [`config`](../ascii-protocol/ch13-command-administration.md#config), [`zkensemble`](../ascii-protocol/ch13-command-administration.md#zkensemble), [`dump`](../ascii-protocol/ch13-command-administration.md#key-dump), [`cmdlog`](../ascii-protocol/ch13-command-administration.md#command-logging), [`lqdetect`](../ascii-protocol/ch13-command-administration.md#long-query-detect), [`reload`](../ascii-protocol/ch13-command-administration.md#reload), [`shutdown`](../ascii-protocol/ch13-command-administration.md#shutdown) | 서버 관리 명령 | `admin` |
| `version`, `ready`, `quit`, [`help`](../ascii-protocol/ch13-command-administration.md#help), [`stats`](../ascii-protocol/ch13-command-administration.md#stats), [`sasl`](../ascii-protocol/ch12-command-sasl.md) | 권한 없이 수행 가능 | `-` |

권한이 없는 명령을 수행하려고 하면, ARCUS 서버는 `CLIENT_ERROR unauthorized` 응답을 반환한다.

### logAll 속성

사용자 계정에 `logall`을 추가로 지정하면, 해당 계정으로 수행하는 모든 명령을 감사로그로 기록한다.

### 감사 로그

ARCUS 서버는 인증과 권한에 관하여 아래의 감사 로그로 기록한다.
- 인증 성공/실패 내역
- 권한이 없는 명령의 수행 내역
- `logAll` 속성이 지정된 사용자 계정으로 수행한 모든 명령의 수행 내역

참고 사항으로, 권한 없이 수행가능한 명령의 수행에 대해서는 감사 로그에 기록하지 않는다.

## ACL 그룹 관리

### arcusctl 도구

ACL 그룹과 사용자 계정/권한 관리를 위한 도구로 `arcusctl`를 제공하며, 아래 기능을 지원한다.
- ACL group의 생성/제거 (ACL group의 계정 관리자 생성/제거 포함)
- ACL group에 속한 사용자 계정 생성/제거/변경

`arcusctl` 도구의 자세한 내용은 [`arcusctl github`](https://github.com/jam2in/arcusctl)을 참고 바란다.

### 계정 정보의 ZooKeeper 저장 구조

ACL 그룹과 사용자 계정 정보는 고가용성이 보장되는 ZooKeeper에 저장하고 조회한다.

ZooKeeper에 저장된 ACL 그룹과 사용자 계정의 예시로, 아래 그림은 2개 ACL 그룹(`prod`, `dev`) 정보를 보인다.
- `/arcus_acl/<group>` 하위 znode는 ZooKeeper ACL을 설정하여 권한을 가진 경우에만 수정할 수 있도록 한다.
  - `prod` 그룹에 대해 `alice` 계정 관리자가 사용자 계정에 대한 모든 권한을 가지며, 그 외는 읽기만 가능하다.
  - `dev` 그룹에 대해 `bob` 계정 관리자가 사용자 계정에 대한 모든 권한을 가지며, 그 외는 읽기만 가능하다.
- `/arcus_acl/<group>/<username>`에는 해당 user의 권한 정보를 보관한다.
- `/arcus_acl/<group>/<username>/authPassword`에는 해당 user의 비밀번호를 암호화된 형태로 보관한다.

<img width="2389" height="1356" alt="image" src="https://github.com/user-attachments/assets/42056027-3482-41d9-8656-382c7184773e" />

## ARCUS 서버 운영

### Build

ARCUS Cache Server 빌드 시 configure option으로 `--enable-sasl` 지정 시 SASL 인증 관련 기능을 사용할 수 있다.

```
./configure ... --enable-sasl [--with-sasl=<cyrus-sasl_install_path>]
```

### Running

Cache Server 구동 시 `-z` 옵션과 `-S` 옵션을 함께 지정하여 클라이언트 연결에 대한 SASL 인증을 요구할 수 있다.

```
$INSTALL_PATH/bin/memcached ... -z 127.0.0.1:2181 -S
```

`-S` 옵션 지정하지 않고 구동하더라도 동적으로 SASL 인증을 활성화하기 위한 `config auth on` 기능을 제공한다.
반대로, SASL 인증을 동적으로 비활성화하는 기능은 제공하지 않는다.

SASL 인증 활성화 시 `-z` 옵션으로 지정한 ZooKeeper의 `/arcus_acl_mapping/<service_code>` znode value를 조회하여 ACL Group을 결정한다.

ARCUS Cache Server는 ZooKeeper에 저장된 인증/권한 정보를 가져와서 캐싱하여 사용하며, 마지막 캐싱 시점으로부터 12-24시간 사이 랜덤한 시간이 지난 뒤 캐싱 작업을 재수행한다.
따라서 ACL 사용자 정보를 추가/수정/제거하는 경우 캐시에 반영되어 사용되기까지 최대 24시간이 소요될 수 있다.
필요 시 빠른 반영을 위해 `reload auth` 명령을 제공한다.

### Commands

`--enable-sasl` 설정 시 활성화되는 명령(ascii protocol 기준)은 다음과 같다.

- [`sasl mech`](../ascii-protocol/ch12-command-sasl.md#sasl-mech): 캐시 서버가 지원하는 인증 방식 목록 조회
- [`sasl auth`](../ascii-protocol/ch12-command-sasl.md#sasl-auth): 특정 인증 방식으로 인증 과정 수행
- [`config auth on`](../ascii-protocol/ch13-command-administration.md#config-auth): 동적으로 SASL 인증을 활성화
- [`reload auth`](../ascii-protocol/ch13-command-administration.md#reload): 외부에 저장된 인증/권한 정보를 로딩하여 캐싱

### Audit Log

감사 로그는 `SECURITY_EVENT` keyword로 시작하며, 아래 상황에서 기록한다.

- 인증 성공/실패
```
SECURITY_EVENT client=10.0.0.1 user=myapp authentication succeeded
SECURITY_EVENT client=10.0.0.1 user=myapp authentication failed(no mechanism available)
```

- 명령 수행 이력
  - 일반 user: 권한 없는 명령 수행 시도 이력
  - `logall` 설정된 user: 모든 명령 수행 이력

```
SECURITY_EVENT client=10.0.0.1 user=myapp cmd="mop create 0 100 10" unauthorized
SECURITY_EVENT client=10.0.0.1 user= cmd="set kv 0 60 5" unauthorized
SECURITY_EVENT client=10.0.0.1 user=myadmin cmd="set kv 0 60 5" unauthorized
SECURITY_EVENT client=10.0.0.1 user=myadmin cmd="config memlimit" authorized
```

