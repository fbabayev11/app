import { useState, useEffect, useRef } from 'react'

const OPTS = [
  { id:'date-desc', icon:'🕐', label:'Ən yeni',    dir:'↓' },
  { id:'date-asc',  icon:'🕐', label:'Ən köhnə',   dir:'↑' },
  { id:'qty-asc',   icon:'📦', label:'Az məhsul',  dir:'↑' },
  { id:'qty-desc',  icon:'📦', label:'Çox məhsul', dir:'↓' },
  { id:'name-asc',  icon:'🔤', label:'Ad A→Z',     dir:'↑' },
  { id:'name-desc', icon:'🔤', label:'Ad Z→A',     dir:'↓' },
]

export default function SortMenu({ value, onChange }) {
  const [open, setOpen] = useState(false)
  const ref = useRef()

  useEffect(() => {
    function outside(e) { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    if (open) document.addEventListener('click', outside, true)
    return () => document.removeEventListener('click', outside, true)
  }, [open])

  const cur = OPTS.find(o => o.id === value) || OPTS[0]

  return (
    <div className="sort-wrap" ref={ref}>
      <button className={`flt-btn${value !== 'date-desc' ? ' on' : ''}`} onClick={() => setOpen(o => !o)}>
        ↕ {value !== 'date-desc' ? cur.label : 'Sırala'}
      </button>
      {open && (
        <div className="sort-drop">
          {OPTS.map(o => (
            <button key={o.id} className={`sort-opt${value===o.id?' active':''}`}
              onClick={() => { onChange(o.id); setOpen(false) }}>
              <span className="si">{o.icon}</span>
              {o.label}
              <span className="sd">{o.dir}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
