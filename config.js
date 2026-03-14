import { useState, useEffect, useRef } from 'react'

const MONTHS = ['Yanvar','Fevral','Mart','Aprel','May','İyun','İyul','Avqust','Sentyabr','Oktyabr','Noyabr','Dekabr']

export default function Calendar({ selDate, onSelect, orderDays = [] }) {
  const [open, setOpen] = useState(false)
  const [y, setY] = useState(new Date().getFullYear())
  const [m, setM] = useState(new Date().getMonth())
  const ref = useRef()

  useEffect(() => {
    function outside(e) { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    if (open) document.addEventListener('click', outside, true)
    return () => document.removeEventListener('click', outside, true)
  }, [open])

  function nav(d) {
    let nm = m + d, ny = y
    if (nm < 0) { nm = 11; ny-- }
    if (nm > 11) { nm = 0; ny++ }
    setY(ny); setM(nm)
  }

  const today = new Date(); today.setHours(0,0,0,0)
  const offset = (new Date(y, m, 1).getDay() + 6) % 7
  const days = new Date(y, m+1, 0).getDate()
  const oDays = new Set(orderDays)

  const label = selDate
    ? `${selDate.slice(8)}/${selDate.slice(5,7)}`
    : 'Tarix'

  return (
    <div className="cal-wrap" ref={ref}>
      <button className={`flt-btn${selDate ? ' on' : ''}`} onClick={() => setOpen(o => !o)}>
        📅 {label}
      </button>
      {open && (
        <div className="cal-drop">
          <div className="cal-head">
            <button className="cal-nav" onClick={() => nav(-1)}>‹</button>
            <span className="cal-head-title">{MONTHS[m]} {y}</span>
            <button className="cal-nav" onClick={() => nav(1)}>›</button>
          </div>
          <div className="cal-wdays">
            {['Be','Ba','Ça','Cü','Ca','Şə','Bz'].map(d => <span key={d}>{d}</span>)}
          </div>
          <div className="cal-grid">
            {Array(offset).fill(null).map((_, i) => <button key={`e${i}`} className="cd cd-empty" />)}
            {Array(days).fill(null).map((_, i) => {
              const d = i + 1
              const ds = `${y}-${String(m+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`
              const dt = new Date(y, m, d)
              let cls = 'cd'
              if (dt.getTime() === today.getTime()) cls += ' cd-today'
              if (ds === selDate) cls += ' cd-sel'
              return (
                <button key={d} className={cls} onClick={() => { onSelect(ds === selDate ? null : ds); setOpen(false) }}>
                  {d}
                  {oDays.has(ds) && <span className="cd-dot" />}
                </button>
              )
            })}
          </div>
          <div className="cal-footer">
            <span>{selDate ? `${selDate.slice(8)}/${selDate.slice(5,7)}/${selDate.slice(0,4)}` : 'Seçilməyib'}</span>
            <button className="cal-reset" onClick={() => { onSelect(null); setOpen(false) }}>Sıfırla</button>
          </div>
        </div>
      )}
    </div>
  )
}
