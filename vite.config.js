import { useState, useEffect } from 'react'
import { API } from '../config'

export default function ArchivePage() {
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)

  async function load() {
    setLoading(true)
    try {
      const r = await fetch(`${API}/archive`)
      if (r.ok) setItems(await r.json())
    } catch {}
    setLoading(false)
  }

  useEffect(() => { load() }, [])

  async function remove(id) {
    await fetch(`${API}/archive/${id}`, { method: 'DELETE' })
    setItems(prev => prev.filter(a => a.id !== id))
  }

  if (loading) return <div className="loading"><div className="spinner" /></div>

  return (
    <>
      <div className="refresh-row">
        <h2>Arxiv</h2>
        <button className="btn btn-ghost btn-sm" onClick={load}>↻ Yenilə</button>
      </div>
      {!items.length ? (
        <div className="empty"><div className="empty-icon">🗂</div><h3>Arxiv boşdur</h3></div>
      ) : (
        <div className="archive-list">
          {items.map(a => (
            <div key={a.id} className="arc-row">
              <div className="arc-left">
                <span className="done-badge">✓ Göndərildi</span>
                <span className="arc-num">{a.name}</span>
                <span className="arc-time">{a.archived_at}</span>
              </div>
              <button className="btn btn-ghost btn-sm" onClick={() => remove(a.id)}>Sil</button>
            </div>
          ))}
        </div>
      )}
    </>
  )
}
