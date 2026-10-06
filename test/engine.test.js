'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { analyze } = require('../src/verifier/engine');
const scenarios = require('../src/scenarios');

const base = (submissions, extra = {}) => ({
  queues: ['Q0', 'Q1', 'Q2'],
  buffers: [{ name: 'f', length: 16 }],
  submissions,
  ...extra
});

test('缺少获取的跨队列读取 -> MISSING_ACQUIRE（422 类拒绝）', () => {
  const r = analyze(scenarios.missingAcquire.input);
  assert.equal(r.ok, false);
  assert.equal(r.code, 'MISSING_ACQUIRE');
  assert.equal(r.submissionId, 'E1');
  assert.equal(r.buffer, 'frameA');
  assert.equal(r.currentOwner, 'decode-q');
});

test('完整移交（释放+信号量+获取）后读取 -> 通过，呈现版本与移交证据', () => {
  const r = analyze(scenarios.completeTransfer.input);
  assert.equal(r.ok, true, JSON.stringify(r));
  assert.deepEqual(r.executableOrder.map((x) => x.id), ['D1', 'E1']);

  const e1 = r.affectedRanges.E1;
  const read = e1.reads[0];
  assert.equal(read.buffer, 'frameA');
  assert.deepEqual([read.start, read.end], [0, 8]);
  assert.equal(read.version, 1);
  assert.ok(read.evidence, '跨队列取得的读必须携带移交证据');
  assert.equal(read.evidence.fromQueue, 'decode-q');
  assert.equal(read.evidence.toQueue, 'encode-q');
  assert.equal(read.evidence.releaseBy, 'D1');
  assert.equal(read.evidence.acquireBy, 'E1');
  assert.equal(read.evidence.via.kind, 'semaphore');

  assert.equal(r.transfers.length, 1);
  assert.deepEqual([r.transfers[0].start, r.transfers[0].end], [0, 8]);
});

test('三队列中继：区间经只等待/递增时间线的中继提交移交，证据须为完整同步链', () => {
  const input = scenarios.relayTransfer.input;
  const r = analyze(input);
  assert.equal(r.ok, true, JSON.stringify(r));

  // 稳定顺序：D1（释放并递增 timeline/decode）→ R1（中继）→ E1（获取并读取）
  assert.deepEqual(r.executableOrder.map((x) => x.id), ['D1', 'R1', 'E1']);

  const read = r.affectedRanges.E1.reads[0];
  assert.equal(read.buffer, 'frameA');
  assert.deepEqual([read.start, read.end], [0, 8]);
  assert.equal(read.version, 1, '最终读取方读到的仍是 v1 原帧');

  // 完整移交链：逐跳连续，包含参与提交与每跳信号量边及对应值
  assert.equal(r.transfers.length, 1);
  const transfer = r.transfers[0];
  assert.deepEqual([transfer.start, transfer.end], [0, 8]);
  assert.equal(transfer.version, 1);
  const evidence = transfer.evidence;
  assert.equal(evidence.releaseBy, 'D1');
  assert.equal(evidence.acquireBy, 'E1');
  assert.equal(evidence.fromQueue, 'decode-q');
  assert.equal(evidence.toQueue, 'encode-q');

  const via = evidence.via;
  assert.equal(via.kind, 'sync-path');
  assert.deepEqual(via.submissions, ['D1', 'R1', 'E1']);
  assert.equal(via.hops.length, 2);
  assert.deepEqual(via.hops[0], { kind: 'semaphore', from: 'D1', to: 'R1', semaphore: 'timeline/decode', value: 1 });
  assert.deepEqual(via.hops[1], { kind: 'semaphore', from: 'R1', to: 'E1', semaphore: 'timeline/correct', value: 1 });
  // 读取证据沿用同一条完整移交链（不能只剩中继到读取方的最后一段）
  assert.deepEqual(read.evidence.via, via);

  // 顺序与路径复算稳定：重复分析结果一致
  const again = analyze(input);
  assert.deepEqual(again.executableOrder.map((x) => x.id), ['D1', 'R1', 'E1']);
  assert.deepEqual(again.transfers[0].evidence.via, via);
});

test('纯中继提交（operations 为空数组）通过结构校验', () => {
  const r = analyze({
    queues: ['Q0', 'Q1'],
    buffers: [{ name: 'f', length: 4 }],
    submissions: [
      { id: 'A', queue: 'Q0', signal: { semaphore: 't' }, operations: [] },
      { id: 'B', queue: 'Q1', wait: { semaphore: 't', value: 1 }, operations: [] }
    ]
  });
  assert.equal(r.ok, true, JSON.stringify(r));
});

test('同队列顺序读：后者可见前者的写，无需移交', () => {
  const r = analyze(base([
    { id: 'A', queue: 'Q0', operations: [{ type: 'write', buffer: 'f', offset: 0, length: 4 }] },
    { id: 'B', queue: 'Q0', operations: [{ type: 'read', buffer: 'f', offset: 0, length: 4 }] }
  ]));
  assert.equal(r.ok, true, JSON.stringify(r));
  assert.deepEqual(r.executableOrder.map((x) => x.id), ['A', 'B']);
  assert.equal(r.affectedRanges.B.reads[0].version, 1);
});

test('等待无满足来源 -> UNSATISFIED_WAIT', () => {
  const r = analyze(base([
    { id: 'A', queue: 'Q1', wait: { semaphore: 't', value: 2 }, operations: [{ type: 'read', buffer: 'f', offset: 0, length: 1 }] }
  ]));
  assert.equal(r.ok, false);
  assert.equal(r.code, 'UNSATISFIED_WAIT');
  assert.equal(r.available, 0);
});

test('信号量等待成环 -> DEADLOCK_CYCLE，环中提交与区间被定位', () => {
  const r = analyze(scenarios.deadlock.input);
  assert.equal(r.ok, false);
  assert.equal(r.code, 'DEADLOCK_CYCLE');
  assert.deepEqual(new Set(r.cycle), new Set(['D1', 'D2']));
  assert.ok(r.touchedRanges.D1.some((x) => x.buffer === 'frameA'));
});

test('跨队列无释放的读取 -> MISSING_RELEASE', () => {
  const r = analyze(base([
    { id: 'A', queue: 'Q0', signal: { semaphore: 't' }, operations: [{ type: 'write', buffer: 'f', offset: 0, length: 2 }] },
    { id: 'B', queue: 'Q1', wait: { semaphore: 't', value: 1 }, operations: [{ type: 'read', buffer: 'f', offset: 0, length: 2 }] }
  ]));
  assert.equal(r.ok, false);
  assert.equal(r.code, 'MISSING_RELEASE');
  assert.equal(r.submissionId, 'B');
});

test('非属主写入 -> WRONG_OWNER', () => {
  const r = analyze(base([
    { id: 'A', queue: 'Q0', signal: { semaphore: 't' }, operations: [{ type: 'write', buffer: 'f', offset: 0, length: 2 }] },
    { id: 'B', queue: 'Q1', wait: { semaphore: 't', value: 1 }, operations: [{ type: 'write', buffer: 'f', offset: 0, length: 2 }] }
  ]));
  assert.equal(r.ok, false);
  assert.equal(r.code, 'WRONG_OWNER');
  assert.equal(r.currentOwner, 'Q0');
});

test('过期版本获取：释放后属主又写新版本 -> STALE_VERSION', () => {
  const r = analyze(base([
    {
      id: 'A', queue: 'Q0', signal: { semaphore: 't' },
      operations: [
        { type: 'write', buffer: 'f', offset: 0, length: 2 },
        { type: 'release', buffer: 'f', offset: 0, length: 2 },
        { type: 'write', buffer: 'f', offset: 0, length: 2 }
      ]
    },
    { id: 'B', queue: 'Q1', wait: { semaphore: 't', value: 1 }, operations: [{ type: 'acquire', buffer: 'f', offset: 0, length: 2 }] }
  ]));
  assert.equal(r.ok, false);
  assert.equal(r.code, 'STALE_VERSION');
  assert.equal(r.observedVersion, 1);
  assert.equal(r.expectedVersion, 2);
});

test('信号量传递先后（Q0→Q1→Q2）支撑跨队列完整移交链', () => {
  const r = analyze(base([
    {
      id: 'A', queue: 'Q0', signal: { semaphore: 'a' },
      operations: [{ type: 'write', buffer: 'f', offset: 0, length: 2 }, { type: 'release', buffer: 'f', offset: 0, length: 2 }]
    },
    {
      id: 'B', queue: 'Q1', wait: { semaphore: 'a', value: 1 }, signal: { semaphore: 'b' },
      operations: [
        { type: 'acquire', buffer: 'f', offset: 0, length: 2 },
        { type: 'read', buffer: 'f', offset: 0, length: 2 },
        { type: 'release', buffer: 'f', offset: 0, length: 2 }
      ]
    },
    {
      id: 'C', queue: 'Q2', wait: { semaphore: 'b', value: 1 },
      operations: [{ type: 'acquire', buffer: 'f', offset: 0, length: 2 }, { type: 'read', buffer: 'f', offset: 0, length: 2 }]
    }
  ]));
  assert.equal(r.ok, true, JSON.stringify(r));
  assert.deepEqual(r.executableOrder.map((x) => x.id), ['A', 'B', 'C']);
  assert.equal(r.affectedRanges.C.reads[0].version, 1);
  // 两段直接移交各自保持简洁的单跳信号量证据（两次移交作用于同一区间，按单元分段）
  const abTransfers = r.transfers.filter((t) => t.evidence.releaseBy === 'A');
  const bcTransfers = r.transfers.filter((t) => t.evidence.releaseBy === 'B');
  assert.ok(abTransfers.length >= 1 && bcTransfers.length >= 1);
  assert.deepEqual(abTransfers[0].evidence.via, { kind: 'semaphore', from: 'A', to: 'B', semaphore: 'a', value: 1 });
  assert.deepEqual(bcTransfers[0].evidence.via, { kind: 'semaphore', from: 'B', to: 'C', semaphore: 'b', value: 1 });
});

test('等待的信号量与释放提交无先后关系 -> ORDERING_MISSING（首个无序读写）', () => {
  const r = analyze(base([
    // S1 发出被等待的信号，但只操作无关单元
    { id: 'S1', queue: 'Q0', signal: { semaphore: 't' }, operations: [{ type: 'write', buffer: 'f', offset: 10, length: 1 }] },
    // S2 之后才写并释放，未与 S1 建立信号关系
    {
      id: 'S2', queue: 'Q0',
      operations: [{ type: 'write', buffer: 'f', offset: 0, length: 2 }, { type: 'release', buffer: 'f', offset: 0, length: 2 }]
    },
    // S3 等到的是 S1 的信号，释放方 S2 并不先于 S3 -> 无序
    { id: 'S3', queue: 'Q1', wait: { semaphore: 't', value: 1 }, operations: [{ type: 'read', buffer: 'f', offset: 0, length: 2 }] }
  ]));
  assert.equal(r.ok, false);
  assert.equal(r.code, 'ORDERING_MISSING');
  assert.equal(r.submissionId, 'S3');
  assert.equal(r.releaseBy, 'S2');
});

test('读取从未写入的单元 -> NO_OWNER', () => {
  const r = analyze(base([
    { id: 'A', queue: 'Q0', operations: [{ type: 'read', buffer: 'f', offset: 3, length: 1 }] }
  ]));
  assert.equal(r.ok, false);
  assert.equal(r.code, 'NO_OWNER');
});

test('结构校验：超上限与越界区间被拒绝', () => {
  const tooManyQueues = {
    queues: ['a', 'b', 'c', 'd'],
    buffers: [{ name: 'f', length: 4 }],
    submissions: []
  };
  assert.equal(analyze(tooManyQueues).code, 'STRUCTURE_INVALID');

  const overrun = base([
    { id: 'A', queue: 'Q0', operations: [{ type: 'write', buffer: 'f', offset: 14, length: 4 }] }
  ]);
  assert.equal(analyze(overrun).code, 'STRUCTURE_INVALID');
});
