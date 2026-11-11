import React, { useEffect, useState } from "react";
import {
  BarChart, Bar, PieChart, Pie, Cell, LineChart, Line, XAxis, YAxis, Tooltip, Legend,
  CartesianGrid, ResponsiveContainer,
} from "recharts";

const COLORS = { positive: "#1f9d55", negative: "#d64545", neutral: "#9aa5b1" };
const get = (path) => fetch(path).then((r) => r.json());

function useApi(path, every = 5000) {
  const [data, setData] = useState(null);
  useEffect(() => {
    let live = true;
    const load = () => get(path).then((d) => live && setData(d)).catch(() => {});
    load();
    const t = setInterval(load, every);
    return () => { live = false; clearInterval(t); };
  }, [path, every]);
  return data;
}

const Card = ({ title, wide, children }) => (
  <section className={"card" + (wide ? " wide" : "")}><h2>{title}</h2>{children}</section>
);

const stack = ["positive", "negative", "neutral"].map((k) => (
  <Bar key={k} dataKey={k} stackId="a" fill={COLORS[k]} />
));

export default function App() {
  const [view, setView] = useState(""); // "" = all sources
  const f = view ? `?source=${view}` : "";
  const summary = useApi("/api/summary" + f);
  const sentiment = useApi("/api/sentiment" + f);
  const platforms = useApi("/api/platforms"); // always all sources (platform comparison + dropdown)
  const series = useApi("/api/timeseries" + f);
  const tags = useApi("/api/hashtags" + f);
  const words = useApi("/api/keywords" + f);
  const aspects = useApi("/api/aspects" + f);
  const langs = useApi("/api/languages" + f);
  const topics = useApi("/api/topics", 15000);
  const history = useApi("/api/history", 15000);

  const [live, setLive] = useState(null);
  useEffect(() => {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws/live${f}`);
    ws.onmessage = (e) => setLive(JSON.parse(e.data));
    return () => ws.close();
  }, [f]);

  const [q, setQ] = useState(""); const [src, setSrc] = useState(""); const [sen, setSen] = useState("");
  const [found, setFound] = useState(null);
  const search = () => get(`/api/search?q=${encodeURIComponent(q)}&source=${src}&sentiment=${sen}`).then(setFound);

  return (
    <>
      <header>
        <h1>Multi-Source Social Media Trend &amp; Sentiment Dashboard</h1>
        <small>Kafka → Spark → HBase → FastAPI → React. Sources: Hacker News (live API) + Sentiment140 (public tweet dataset).</small>
      </header>
      <main>
        <Card title="Data source" wide>
          <select value={view} onChange={(e) => setView(e.target.value)}>
            <option value="">all sources</option>
            {(platforms || []).map((p) => <option key={p.source} value={p.source}>{p.source} ({p.total})</option>)}
          </select>
          <small> Charts below are filtered to this source (Platform comparison, Topics and Historical analytics always cover all sources).</small>
        </Card>

        <Card title="Overview" wide>
          <div className="stats">
            <div className="stat"><b>{summary?.total ?? "…"}</b>Total posts</div>
            <div className="stat"><b className="pos">{summary?.positive ?? "…"}</b>Positive</div>
            <div className="stat"><b className="neg">{summary?.negative ?? "…"}</b>Negative</div>
            <div className="stat"><b className="neu">{summary?.neutral ?? "…"}</b>Neutral</div>
          </div>
        </Card>

        <Card title="Sentiment distribution">
          <ResponsiveContainer width="100%" height={240}>
            <PieChart><Pie data={sentiment || []} dataKey="value" nameKey="name" label>
              {(sentiment || []).map((s) => <Cell key={s.name} fill={COLORS[s.name]} />)}
            </Pie><Tooltip /><Legend /></PieChart>
          </ResponsiveContainer>
        </Card>

        <Card title="Platform comparison">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={platforms || []}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="source" /><YAxis />
              <Tooltip /><Legend />{stack}</BarChart>
          </ResponsiveContainer>
        </Card>

        <Card title="Sentiment over time (by day)" wide>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={series || []}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="day" /><YAxis />
              <Tooltip /><Legend />
              {["positive", "negative", "neutral"].map((k) => <Line key={k} dataKey={k} stroke={COLORS[k]} dot={false} />)}
            </LineChart>
          </ResponsiveContainer>
        </Card>

        <Card title="Trending hashtags (last day vs earlier baseline)">
          <table><thead><tr><th>Tag</th><th>Recent</th><th>Base/day</th><th>Trend score</th></tr></thead>
            <tbody>{(tags || []).map((t) => (
              <tr key={t.tag}><td>{t.tag}</td><td>{t.recent}</td><td>{t.baseline_per_day}</td><td>{t.trend_score}</td></tr>
            ))}</tbody></table>
        </Card>

        <Card title="Keyword analysis">
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={words || []} layout="vertical"><XAxis type="number" /><YAxis type="category" dataKey="word" width={90} interval={0} />
              <Tooltip /><Bar dataKey="count" fill="#3b6ea5" /></BarChart>
          </ResponsiveContainer>
        </Card>

        <Card title="Aspect-based sentiment">
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={aspects || []}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="aspect" /><YAxis />
              <Tooltip /><Legend />{stack}</BarChart>
          </ResponsiveContainer>
        </Card>

        <Card title="Topics (Spark batch LDA)">
          {topics && topics.length === 0 && <p>Run the batch job to populate topics.</p>}
          <ul>{(topics || []).map((t) => <li key={t.topic}><b>{t.topic}</b> ({t.docs} posts): {t.words.join(", ")}</li>)}</ul>
        </Card>

        <Card title="Language analytics">
          <table><tbody>{(langs || []).map((l) => <tr key={l.lang}><td>{l.lang}</td><td>{l.count}</td></tr>)}</tbody></table>
          <small>English-only analysis for now; other languages are detected but not scored reliably.</small>
        </Card>

        <Card title="Historical analytics (batch aggregates)" wide>
          <table><thead><tr><th>Key</th><th>Pos</th><th>Neg</th><th>Neu</th></tr></thead>
            <tbody>{(history || []).slice(0, 15).map((h) => (
              <tr key={h.key}><td>{h.key}</td><td>{h.positive}</td><td>{h.negative}</td><td>{h.neutral}</td></tr>
            ))}</tbody></table>
        </Card>

        <Card title="Live feed (WebSocket)" wide>
          <div>Live total: <b>{live?.summary.total ?? "connecting…"}</b></div>
          <ul className="live">{(live?.latest || []).map((p) => (
            <li key={p.id}><b className={p.label.slice(0, 3)}>{p.label}</b> [{p.source}] {p.text.slice(0, 110)}</li>
          ))}</ul>
        </Card>

        <Card title="Search and filter" wide>
          <input placeholder="text contains…" value={q} onChange={(e) => setQ(e.target.value)} />
          <select value={src} onChange={(e) => setSrc(e.target.value)}>
            <option value="">all sources</option>
            {(platforms || []).map((p) => <option key={p.source}>{p.source}</option>)}
          </select>
          <select value={sen} onChange={(e) => setSen(e.target.value)}>
            <option value="">any sentiment</option><option>positive</option><option>negative</option><option>neutral</option>
          </select>
          <button onClick={search}>Search</button>
          {found && <p>{found.count} matches</p>}
          <table><tbody>{(found?.results || []).slice(0, 20).map((r) => (
            <tr key={r.id}><td className={r.label.slice(0, 3)}>{r.label}</td><td>{r.source}</td><td>{r.text.slice(0, 120)}</td></tr>
          ))}</tbody></table>
        </Card>
      </main>
    </>
  );
}
