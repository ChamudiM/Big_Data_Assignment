const byId = id => document.getElementById(id);
const money = value => new Intl.NumberFormat(undefined, {style: 'currency', currency: 'USD'}).format(value);
const time = value => value ? new Date(value).toLocaleTimeString() : '—';

async function refresh() {
    try {
        const response = await fetch('/api/operations/summary', {cache: 'no-store'});
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        render(await response.json());
        byId('status-dot').classList.add('online');
        byId('status-text').textContent = 'Live';
    } catch (error) {
        byId('status-dot').classList.remove('online');
        byId('status-text').textContent = 'Disconnected';
    }
}

function render(data) {
    byId('processed-count').textContent = data.processedOrders.toLocaleString();
    byId('product-count').textContent = data.averages.length;
    byId('retry-count').textContent = data.retries.length;
    byId('dlq-count').textContent = data.deadLetters.length;
    byId('updated-at').textContent = `Updated ${time(data.generatedAt)}`;

    const grid = byId('average-grid');
    grid.innerHTML = '';
    if (!data.averages.length) grid.innerHTML = '<p class="empty">Waiting for aggregate records…</p>';
    data.averages.forEach(item => {
        const card = byId('average-card-template').content.cloneNode(true);
        card.querySelector('.product').textContent = item.product;
        card.querySelector('.orders').textContent = `${item.orderCount} orders`;
        card.querySelector('.average').textContent = money(item.averagePrice);
        card.querySelector('.total').textContent = `${money(item.totalPrice)} total`;
        grid.appendChild(card);
    });

    const retries = byId('retry-body');
    retries.innerHTML = data.retries.length ? '' : '<tr><td colspan="4" class="empty">No retries observed</td></tr>';
    data.retries.slice(0, 20).forEach(item => retries.insertAdjacentHTML('beforeend',
        `<tr><td>${escapeHtml(item.orderId)}</td><td>${item.attempt}</td><td>${escapeHtml(item.reason || item.exceptionType)}</td><td>${time(item.occurredAt)}</td></tr>`));

    const dlq = byId('dlq-body');
    dlq.innerHTML = data.deadLetters.length ? '' : '<tr><td colspan="4" class="empty">DLQ is empty</td></tr>';
    data.deadLetters.forEach(item => dlq.insertAdjacentHTML('beforeend',
        `<tr><td>${escapeHtml(item.orderId)}</td><td>${escapeHtml(item.product)}</td><td>${escapeHtml(item.reason || 'Processing failed')}</td>` +
        `<td><button onclick="replay('${encodeURIComponent(item.orderId)}')">Replay</button></td></tr>`));
}

async function replay(orderId) {
    const response = await fetch(`/api/operations/dlq/${orderId}/replay`, {method: 'POST'});
    if (!response.ok) alert(`Replay failed: ${await response.text()}`);
    await refresh();
}

byId('order-form').addEventListener('submit', async event => {
    event.preventDefault();
    const message = byId('form-message');
    message.textContent = 'Publishing…';
    const response = await fetch('/api/operations/orders', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({product: byId('product').value, price: Number(byId('price').value)})
    });
    const result = await response.json();
    message.textContent = response.ok ? `Published ${result.orderId}` : 'Publish failed';
    setTimeout(() => message.textContent = '', 4000);
});

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>'"]/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));
}

refresh();
setInterval(refresh, 2000);
