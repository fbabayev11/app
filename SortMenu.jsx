import { useState, useEffect, useMemo } from 'react'
import { API } from './config'
import ConfigPage from './pages/ConfigPage'
import OrdersPage from './pages/OrdersPage'
import ArchivePage from './pages/ArchivePage'
import ImagesPage from './pages/ImagesPage'

export default function App() {
  const [tab, setTab]         = useState('orders')
  const [shopName, setShopName] = useState('')
  const [configured, setConfigured] = useState(null)
  const [orders, setOrders]   = useState([])
  const [showConfig, setShowConfig] = useState(false)

  useEffect(() => {
    fetch(`${API}/config`).then(r=>r.json()).then(d=>{
      setConfigured(d.configured)
      if (d.configured) setShopName(d.shop_name || d.shop)
    }).catch(()=>setConfigured(false))
  }, [])

  const productNames = useMemo(() => {
    const s = new Set()
    orders.forEach(o => o.items?.forEach(i => {
      if (i.is_custom_box) (i.box_contents||[]).forEach(c => { if(c.name) s.add(c.name) })
      else if (i.name) s.add(i.name)
    }))
    return [...s].sort()
  }, [orders])

  if (configured === null) return (
    <div className="loading" style={{paddingTop:120}}>
      <div className="spinner" />
    </div>
  )

  if (!configured || showConfig) return (
    <main>
      <ConfigPage onSave={name => { setShopName(name); setConfigured(true); setShowConfig(false) }} />
    </main>
  )

  return (
    <>
      <header>
        <div className="logo">
          <div className="logo-icon">📦</div>
          <span className="logo-text">Qablaşdırma</span>
        </div>
        <div style={{display:'flex',alignItems:'center',gap:10}}>
          <span className="shop-badge">{shopName}</span>
          <button className="btn btn-ghost btn-sm" onClick={()=>setShowConfig(true)}>⚙ Ayarlar</button>
        </div>
      </header>

      <div className="tabs">
        {[
          { id:'orders',  label:'Sifarişlər' },
          { id:'archive', label:'Arxiv' },
          { id:'images',  label:'🖼 Şəkillər' },
        ].map(t => (
          <button key={t.id} className={`tab${tab===t.id?' active':''}`} onClick={()=>setTab(t.id)}>
            {t.label}
          </button>
        ))}
      </div>

      <main>
        {tab === 'orders'  && <OrdersPage />}
        {tab === 'archive' && <ArchivePage />}
        {tab === 'images'  && <ImagesPage productNames={productNames} />}
      </main>
    </>
  )
}
