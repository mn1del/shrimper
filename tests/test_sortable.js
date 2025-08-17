const test = require('node:test');
const assert = require('node:assert');
const { compareValues } = require('../app/static/sortable.js');

test('ascending sort places empty strings last', () => {
  const values = ['b', '', 'a'];
  const sorted = values.slice().sort((a, b) => compareValues(a, b, 'asc'));
  assert.deepStrictEqual(sorted, ['a', 'b', '']);
});

test('descending sort places empty strings last', () => {
  const values = ['b', '', 'a'];
  const sorted = values.slice().sort((a, b) => compareValues(a, b, 'desc'));
  assert.deepStrictEqual(sorted, ['b', 'a', '']);
});
