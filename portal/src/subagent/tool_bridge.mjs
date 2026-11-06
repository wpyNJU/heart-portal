import net from 'node:net';
const definitions = /*PORTAL_TOOL_DEFINITIONS*/[];
const mailbox = /*PORTAL_TEAM_MAILBOX*/null;
async function call(name, args, signal) {
      const value = await new Promise((resolve, reject) => {
        let data = ''; let finished = false;
        const socket = net.createConnection({ host: '127.0.0.1', port: Number(process.env.PORTAL_TOOL_PORT) });
        const finish = (error, value) => {
          if (finished) return; finished = true;
          signal?.removeEventListener('abort', abort); socket.destroy();
          if (error) reject(error); else resolve(value);
        };
        const abort = () => finish(new Error('Tool call cancelled'));
        if (signal?.aborted) { abort(); return; }
        signal?.addEventListener('abort', abort, { once: true });
        socket.setTimeout(430000, () => finish(new Error('Portal tool timed out')));
        socket.setEncoding('utf8');
        socket.on('connect', () => socket.write(JSON.stringify({token:process.env.PORTAL_TOOL_TOKEN, tool:name, arguments:args})+'\n'));
        socket.on('data', chunk => {
          data += chunk;
          if (data.length > 16000000) { finish(new Error('Portal result too large')); return; }
          if (data.includes('\n')) { try { finish(null, JSON.parse(data.split('\n')[0])); } catch (error) { finish(error); } }
        });
        socket.on('error', error => finish(error));
        socket.on('close', () => { if (!finished) finish(new Error('Portal tool broker closed')); });
      });
      if (value.isError) throw new Error(value.content?.filter(x => x.type === 'text').map(x => x.text).join('\n') || 'Portal denied the operation');

  return value;
}

export default function portalTools(pi) {
  const names = definitions.map(tool => tool.name);
  for (const tool of definitions) pi.registerTool({
    name: tool.name, label: tool.name, description: tool.description,
    parameters: tool.inputSchema,
    async execute(_id, args, signal) {
      const value = await call(tool.name, args, signal);
      return { content: value.content, details: {} };
    }
  });
  if (mailbox) {
    let cursor = mailbox.after;
    const received = [];
    // pi's print mode does not drain asynchronous turn_end listeners before
    // unsubscribing from agent_end. The context hook is awaited by the model
    // request itself, so messages arrive safely before the next model step.
    pi.on('context', async event => {
      const response = await call('portal_team', {op:'mailbox',task_id:mailbox.task_id,after:cursor});
      const page = JSON.parse(response.content.find(item => item.type === 'text').text);
      for (const message of page.messages) received.push({
        role:'user', content:`Team message from ${message.sender}:\n${message.text}`,
        timestamp:message.created_ms,
      });
      cursor = page.cursor;
      return {messages:[...event.messages,...received]};
    });
  }
  pi.on('session_start', () => pi.setActiveTools(names));
  pi.on('tool_call', event => names.includes(event.toolName) ? undefined : { block:true, reason:'Only Portal broker tools are allowed' });
}
