import { useState, useEffect, useRef } from 'react'

export default function StockFilter({ excluded, onChange, productNames }) {
  const [open, setOpen] = useState(false)
  const [q, setQ]       = useState('')
  const ref = useRef()

  useEffect(() => {
    function outside(e) { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    if (open) document.addEventListener('click', outside, true)
    return () => document.removeEventListener('click', outside, true)
  }, [open])

  const filtered = q ? productNames.filter(n => n.toLowerCase().includes(q.toLowerCase())) : productNames
  const allSel   = filtered.length > 0 && filtered.every(n => excluded.includes(n))

  function toggle(name) {
    onChange(excluded.includes(name) ? excluded.filter(x => x !== name) : [...excluded, name])
  }
  function toggleAll() {
    if (allSel) onChange(excluded.filter(n => !filtered.includes(n)))
    else onChange([...new Set([...excluded, ...filtered])])
  }

  return (
    <div className="stock-wrap" ref={ref}>
      <button className={`flt-btn${excluded.length ? ' on' : ''}`} onClick={() => setOpen(o => !o)}>
        🚫 {excluded.length ? `${excluded.length} məhsul` : 'Stok filteri'}
      </button>
      {open && (
        <div className="stock-drop">
          <h4 style={{fontSize:11,fontWeight:700,color:'var(--muted)',textTransform:'uppercase',letterSpacing:.5,marginBottom:10}}>
            Hansı məhsul olmayan sifarişlər?
          </h4>
          <div className="prod-chip-search">
            <span style={{color:'var(--muted)',fontSize:13}}>🔍</span>
            <input value={q} onChange={e => setQ(e.target.value)} placeholder="Məhsul axtar..." />
          </div>
          {filtered.length > 1 && (
            <button onClick={toggleAll} style={{
              display:'flex',alignItems:'center',gap:6,width:'100%',padding:'7px 10px',
              borderRadius:8,border:`1px dashed ${allSel?'rgba(248,113,113,.4)':'rgba(245,166,35,.3)'}`,
              background:allSel?'rgba(248,113,113,.06)':'rgba(245,166,35,.05)',
              color:allSel?'var(--red)':'var(--accent)',fontSize:12,fontWeight:700,
              cursor:'pointer',marginBottom:6,fontFamily:'var(--sans)'
            }}>
              <span>{allSel ? '✕ Hamısını çıxart' : '✓ Hamısını seç'}</span>
              <span style={{marginLeft:'auto',opacity:.6}}>{filtered.length} məhsul</span>
            </button>
          )}
          <div className="stock-suggestions">
            {filtered.map(n => (
              <button key={n} className={`stock-sugg${excluded.includes(n)?' selected':''}`} onClick={() => toggle(n)}>
                <span>{n}</span>
                <span style={{color: excluded.includes(n)?'var(--red)':'var(--muted)',fontSize:14}}>
                  {excluded.includes(n) ? '✕' : '+'}
                </span>
              </button>
            ))}
          </div>
          {excluded.length > 0 && (
            <div style={{paddingTop:10,borderTop:'1px solid var(--border)',marginTop:8}}>
              <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:8}}>
                <span className="stock-divider" style={{margin:0}}>{excluded.length} məhsul seçilib</span>
                <button onClick={() => onChange([])} style={{
                  background:'rgba(248,113,113,.1)',border:'1px solid rgba(248,113,113,.25)',
                  color:'var(--red)',fontSize:11,fontWeight:700,padding:'4px 10px',
                  borderRadius:6,cursor:'pointer',fontFamily:'var(--sans)'
                }}>✕ Hamısını sil</button>
              </div>
              <div style={{display:'flex',flexWrap:'wrap',gap:5}}>
                {excluded.map((t,i) => (
                  <div key={i} className="stock-tag" onClick={() => toggle(t)}>
                    <span>🚫 {t}</span>
                    <span style={{opacity:.6,fontSize:11}}>✕</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
