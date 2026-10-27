// Visit reporting: the page sends one beacon per browser session (see the
// VISIT TRACKING script in index.html). Visits are queued in Upstash Redis and
// every BATCH_SIZE visits an email summary goes to REPORT_TO via Resend.
//
// No full IP addresses are stored. A per-IP counter (hashed key, 1h expiry)
// is used only for rate limiting.

const BATCH_SIZE = 3;
const REPORT_TO = 'gautam@hyumanx.com';
const FROM = 'Portfolio Visits <visits@gautamtverma.com>';
const MAX_PER_IP_PER_HOUR = 6;
const MAX_EMAILS_PER_DAY = 40;
const QUEUE_KEY = 'visits:pending';

const BOT_UA = /bot|crawl|spider|slurp|preview|headless|lighthouse|pagespeed|facebookexternalhit|embedly|whatsapp|telegram|curl|wget|python|axios|node-fetch|go-http|java\/|monitor|uptime|scan/i;

async function redis(...command) {
    const res = await fetch(process.env.KV_REST_API_URL, {
        method: 'POST',
        headers: { Authorization: `Bearer ${process.env.KV_REST_API_TOKEN}` },
        body: JSON.stringify(command),
    });
    if (!res.ok) throw new Error(`Redis ${command[0]} failed: ${res.status}`);
    return (await res.json()).result;
}

async function redisTransaction(commands) {
    const res = await fetch(`${process.env.KV_REST_API_URL}/multi-exec`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${process.env.KV_REST_API_TOKEN}` },
        body: JSON.stringify(commands),
    });
    if (!res.ok) throw new Error(`Redis transaction failed: ${res.status}`);
    return (await res.json()).map(r => r.result);
}

async function sha256(text) {
    const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
    return [...new Uint8Array(buf)].slice(0, 12).map(b => b.toString(16).padStart(2, '0')).join('');
}

function clip(value, max = 300) {
    return typeof value === 'string' ? value.slice(0, max) : '';
}

function parseUserAgent(ua) {
    const device = /iPad|Tablet/i.test(ua) ? 'Tablet'
        : /Mobi|iPhone|Android/i.test(ua) ? 'Mobile'
        : 'Desktop';
    const os = /Windows NT/i.test(ua) ? 'Windows'
        : /iPhone|iPad|iPod/i.test(ua) ? 'iOS'
        : /Mac OS X/i.test(ua) ? 'macOS'
        : /Android/i.test(ua) ? 'Android'
        : /CrOS/i.test(ua) ? 'ChromeOS'
        : /Linux/i.test(ua) ? 'Linux'
        : 'Other';
    const browser = /Edg\//i.test(ua) ? 'Edge'
        : /OPR\/|Opera/i.test(ua) ? 'Opera'
        : /SamsungBrowser/i.test(ua) ? 'Samsung Internet'
        : /Chrome\/|CriOS/i.test(ua) ? 'Chrome'
        : /Firefox\/|FxiOS/i.test(ua) ? 'Firefox'
        : /Safari\//i.test(ua) ? 'Safari'
        : 'Other';
    return { device, os, browser };
}

function describeSource(referrer, utm) {
    if (utm.source) return [utm.source, utm.medium, utm.campaign].filter(Boolean).join(' / ');
    if (!referrer) return 'Direct (typed URL, bookmark, or app)';
    try {
        const host = new URL(referrer).hostname.replace(/^www\./, '');
        if (/linkedin|lnkd\.in/.test(host)) return `LinkedIn (${host})`;
        if (/google\./.test(host)) return 'Google search';
        if (/bing\.com/.test(host)) return 'Bing search';
        if (/(^|\.)t\.co$|twitter|x\.com/.test(host)) return `X / Twitter (${host})`;
        if (/hyumanx/.test(host)) return `HyumanX site (${host})`;
        return host;
    } catch {
        return clip(referrer, 120);
    }
}

function buildVisit(request, data) {
    const h = request.headers;
    const city = decodeURIComponent(h.get('x-vercel-ip-city') || '');
    const countryCode = h.get('x-vercel-ip-country') || '';
    let country = countryCode;
    try { country = new Intl.DisplayNames(['en'], { type: 'region' }).of(countryCode) || countryCode; } catch {}
    const ua = h.get('user-agent') || '';
    const utm = {
        source: clip(data.utm?.source, 80),
        medium: clip(data.utm?.medium, 80),
        campaign: clip(data.utm?.campaign, 80),
    };
    const clicks = Array.isArray(data.clicks) ? data.clicks.slice(0, 10).map(c => clip(c, 40)) : [];
    return {
        at: new Date().toISOString(),
        location: [city, country].filter(Boolean).join(', ') || 'Unknown',
        source: describeSource(clip(data.referrer, 500), utm),
        referrer: clip(data.referrer, 300),
        ...parseUserAgent(ua),
        screen: clip(data.screen, 20),
        language: clip(data.language, 20),
        timezone: clip(data.timezone, 50),
        page: clip(data.page, 200),
        visitNumber: Number.isInteger(data.visitNumber) ? data.visitNumber : null,
        secondsOnPage: Number.isFinite(data.seconds) ? Math.min(Math.round(data.seconds), 86400) : null,
        scrollDepth: Number.isFinite(data.scroll) ? Math.max(0, Math.min(100, Math.round(data.scroll))) : null,
        clicks,
    };
}

function formatDuration(s) {
    if (s == null) return '—';
    if (s < 60) return `${s}s`;
    return `${Math.floor(s / 60)}m ${s % 60}s`;
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function visitRows(v) {
    const when = new Date(v.at).toLocaleString('en-GB', { timeZone: 'Asia/Dubai', dateStyle: 'medium', timeStyle: 'short' });
    return [
        ['When', `${when} (Dubai time)`],
        ['Location', v.location],
        ['Came from', v.source],
        ['Visitor', v.visitNumber > 1 ? `Returning — visit #${v.visitNumber}` : 'First visit'],
        ['Device', `${v.device} · ${v.os} · ${v.browser}${v.screen ? ` · ${v.screen}` : ''}`],
        ['Time on page', formatDuration(v.secondsOnPage)],
        ['Scrolled', v.scrollDepth == null ? '—' : `${v.scrollDepth}% of the page`],
        ['Clicked', v.clicks.length ? v.clicks.join(', ') : 'Nothing tracked'],
        ['Language / timezone', [v.language, v.timezone].filter(Boolean).join(' · ') || '—'],
        ['Landing page', v.page || '/'],
    ];
}

function buildEmail(visits) {
    const places = [...new Set(visits.map(v => v.location.split(',')[0]))].join(', ');
    const subject = `${visits.length} new visits to gautamtverma.com — ${places}`;
    const cards = visits.map((v, i) => `
        <table cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;max-width:620px;margin:0 0 22px;font-family:Arial,sans-serif;font-size:14px;border:1px solid #E8E4DC">
            <tr><td colspan="2" style="background:#1A1A1A;color:#FAF7F2;font-weight:bold">Visit ${i + 1}</td></tr>
            ${visitRows(v).map(([k, val]) => `<tr><td style="color:#666;width:150px;border-top:1px solid #E8E4DC;vertical-align:top">${k}</td><td style="border-top:1px solid #E8E4DC">${escapeHtml(val)}</td></tr>`).join('')}
        </table>`).join('');
    const html = `<meta charset="utf-8"><div style="font-family:Arial,sans-serif;color:#1A1A1A">
        <p style="font-size:15px">${visits.length} people visited <a href="https://gautamtverma.com" style="color:#2EC4B6">gautamtverma.com</a> since the last report.</p>
        ${cards}
        <p style="font-size:12px;color:#666">Locations are approximate (city-level, from the visitor's network). Your own visits are excluded on any browser where you've opened gautamtverma.com/?notrack once.</p>
    </div>`;
    const text = visits.map((v, i) => `Visit ${i + 1}\n` + visitRows(v).map(([k, val]) => `  ${k}: ${val}`).join('\n')).join('\n\n');
    return { subject, html, text };
}

async function sendReport(visits) {
    const { subject, html, text } = buildEmail(visits);
    const res = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ from: FROM, to: [REPORT_TO], subject, html, text }),
    });
    if (!res.ok) throw new Error(`Resend failed: ${res.status} ${await res.text()}`);
}

export async function POST(request) {
    try {
        const ua = request.headers.get('user-agent') || '';
        if (!ua || BOT_UA.test(ua)) return new Response(null, { status: 204 });

        const raw = await request.text();
        if (raw.length > 4000) return new Response(null, { status: 413 });
        let data;
        try { data = JSON.parse(raw); } catch { return new Response(null, { status: 400 }); }

        const ip = (request.headers.get('x-forwarded-for') || '').split(',')[0].trim();
        const ipKey = `visits:ip:${await sha256(ip + process.env.KV_REST_API_URL)}`;
        const [hits] = await redisTransaction([['INCR', ipKey], ['EXPIRE', ipKey, '3600']]);
        if (hits > MAX_PER_IP_PER_HOUR) return new Response(null, { status: 204 });

        const visit = buildVisit(request, data);
        const queued = await redis('RPUSH', QUEUE_KEY, JSON.stringify(visit));
        if (queued < BATCH_SIZE) return new Response(null, { status: 204 });

        // Atomically take the whole queue so concurrent requests can't double-send.
        const [items] = await redisTransaction([['LRANGE', QUEUE_KEY, '0', '-1'], ['DEL', QUEUE_KEY]]);
        if (!items?.length) return new Response(null, { status: 204 });
        const visits = items.map(s => JSON.parse(s));

        const dayKey = `visits:emails:${new Date().toISOString().slice(0, 10)}`;
        const [sentToday] = await redisTransaction([['INCR', dayKey], ['EXPIRE', dayKey, '172800']]);
        if (sentToday > MAX_EMAILS_PER_DAY) return new Response(null, { status: 204 });

        try {
            await sendReport(visits);
        } catch (err) {
            // Put the visits back so they go out with the next report.
            await redis('LPUSH', QUEUE_KEY, ...items.reverse());
            throw err;
        }
        return new Response(null, { status: 204 });
    } catch (err) {
        console.error('[visit]', err);
        return new Response(null, { status: 204 });
    }
}
