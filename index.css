import { useState } from 'react'
import { FLAG } from '../config'

function ImgEl({ url, cls, ph = '📦' }) {
  const [err, setErr] = useState(false)
  if (url && !err) return <img className={cls} src={url} loading="lazy" onError={() => setErr(true)} />
  return <div className={`${cls}-ph`}>{ph}</div>
}

function NormalItem({ item }) {
  return (
    <div className="item-row">
      <ImgEl url={item.image} cls="item-img" />
      <div className="item-qty-big">{item.quantity}</div>
      <div style={{flex:1}}>
        <div className="item-name">{item.name}</div>
        {item.variant && <div className="item-variant">{item.variant}</div>}
      </div>
      {item.sku && <div className="item-sku">{item.sku}</div>}
    </div>
  )
}

function BoxItem({ item }) {
  return (
    <div className="item-row box-row">
      <div className="box-header">
        <div className="item-qty-big box-qty">{item.quantity}</div>
        <div style={{flex:1}}>
          <div className="item-name">🎁 {item.name}</div>
          <div className="box-label">{item.box_contents.length} fərqli məhsul</div>
        </div>
      </div>
      <div className="box-items">
        {item.box_contents.map((c, i) => (
          <div key={i} className="box-item">
            <ImgEl url={c.image} cls="box-item-img" ph="🍬" />
            <div className="box-item-qty">{c.quantity}</div>
            <div className="box-item-name">{c.name}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

function PackMode({ order, onExit, onArchive }) {
  const flatItems = []
  order.items.forEach(i => {
    if (i.is_custom_box && i.box_contents?.length) {
      i.box_contents.forEach(c => flatItems.push({ name: c.name, qty: c.quantity, img: c.image || '', variant: '' }))
    } else {
      flatItems.push({ name: i.name, qty: i.quantity, img: i.image || '', variant: i.variant || '' })
    }
  })

  const [done, setDone] = useState(new Set())
  const total = flatItems.length
  const doneCount = done.size
  const pct = total ? Math.round(doneCount / total * 100) : 0
  const allDone = doneCount === total

  function toggle(idx) {
    setDone(prev => {
      const next = new Set(prev)
      next.has(idx) ? next.delete(idx) : next.add(idx)
      return next
    })
  }

  return (
    <div className="pack-mode">
      <div className="pack-progress">
        <div className="pack-bar-bg">
          <div className="pack-bar-fill" style={{ width: `${pct}%` }} />
        </div>
        <span className="pack-bar-txt">{doneCount}/{total}</span>
      </div>
      {order.note && <div className="order-note" style={{marginBottom:12}}>📝 {order.note}</div>}
      {flatItems.map((item, idx) => (
        <div key={idx} className={`pack-item${done.has(idx) ? ' done' : ''}`} onClick={() => toggle(idx)}>
          <div className="pack-check">{done.has(idx) ? '✓' : ''}</div>
          <ImgEl url={item.img} cls="pack-item-img" />
          <div className="pack-item-qty">{item.qty}</div>
          <div style={{flex:1}}>
            <div className="pack-item-name">{item.name}</div>
            {item.variant && <div className="pack-item-var">{item.variant}</div>}
          </div>
        </div>
      ))}
      <div className="pack-footer">
        <button className="btn btn-ghost btn-sm" onClick={onExit}>← Geri</button>
        <button className="btn btn-ghost btn-sm" onClick={() => setDone(new Set())}>↺ Sıfırla</button>
        <button
          className="btn btn-green btn-sm"
          style={{ opacity: allDone ? 1 : 0.5 }}
          onClick={e => onArchive(order.id, order.name, e)}
        >✓ Arxivlə</button>
      </div>
    </div>
  )
}

export default function OrderCard({ order, onArchive }) {
  const [open, setOpen]   = useState(false)
  const [pack, setPack]   = useState(false)

  return (
    <div className={`order-card${open ? ' open' : ''}`} id={`card-${order.id}`}>
      <div className="order-head" onClick={() => setOpen(o => !o)}>
        <div className="order-num">{order.name}</div>
        <div className="order-meta">
          <div className="order-customer">{order.customer || '—'}</div>
          <div className="order-info">{[order.city, order.created_at].filter(Boolean).join(' · ')}</div>
        </div>
        <div className="order-count">📦 {order.total_items} əd.</div>
        <div className="order-flag">{FLAG(order.country_code)}</div>
        {order.note && <div className="note-dot" title="Qeyd var" />}
        <div className="chevron" style={{ transform: open ? 'rotate(180deg)' : 'none', transition: 'transform .2s' }}>▼</div>
      </div>

      {open && (
        <div className="order-body">
          {pack ? (
            <PackMode order={order} onExit={() => setPack(false)} onArchive={onArchive} />
          ) : (
            <>
              <div className="items-list">
                {order.items.map((item, i) =>
                  item.is_custom_box && item.box_contents?.length
                    ? <BoxItem key={i} item={item} />
                    : <NormalItem key={i} item={item} />
                )}
              </div>
              {order.note && <div className="order-note">📝 {order.note}</div>}
              <div className="order-actions">
                <button className="btn btn-ghost btn-sm" style={{marginRight:'auto'}} onClick={() => setPack(true)}>
                  📋 Yığmağa başla
                </button>
                <button className="btn btn-green btn-sm" onClick={e => onArchive(order.id, order.name, e)}>
                  ✓ Arxivlə
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
