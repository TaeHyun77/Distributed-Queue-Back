// .claude/hooks/guard-lua-scripts.js
// Lua 스크립트 수정 시 연결된 Kotlin 코드도 함께 수정해야 한다는 경고를 출력한다.
const data = JSON.parse(require('fs').readFileSync('/dev/stdin', 'utf8'));
const filePath = data.tool_input?.file_path || '';

if (filePath.includes('enqueue-or-allow.lua')) {
  console.error('경고: enqueue-or-allow.lua 수정 시 QueueService.registerUserToWaitQueue()의 when 분기를 반드시 함께 수정하세요.');
}
if (filePath.includes('schedule-promote.lua')) {
  console.error('경고: schedule-promote.lua 수정 시 호출부의 반환 값 처리를 반드시 함께 수정하세요.');
}
