export const API = 'https://app-t87p.onrender.com'

export const FLAG = c => ({
  DE:'🇩🇪',AT:'🇦🇹',CH:'🇨🇭',FR:'🇫🇷',NL:'🇳🇱',BE:'🇧🇪',IT:'🇮🇹',ES:'🇪🇸',
  PL:'🇵🇱',AZ:'🇦🇿',TR:'🇹🇷',US:'🇺🇸',GB:'🇬🇧',RU:'🇷🇺',SE:'🇸🇪',CZ:'🇨🇿',
  SK:'🇸🇰',HU:'🇭🇺',RO:'🇷🇴',DK:'🇩🇰',NO:'🇳🇴',FI:'🇫🇮',LT:'🇱🇹',LV:'🇱🇻',
  EE:'🇪🇪',UA:'🇺🇦',GE:'🇬🇪'
}[(c||'').toUpperCase()] || '🌍')
