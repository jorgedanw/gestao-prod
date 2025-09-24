# etl/maps.py
# Mapa de status do legado -> nome claro (igual ao status.ts)
from typing import Optional, Literal, Dict

StatusOP = Literal["ABERTA","ENTRADA_PARCIAL","FINALIZADA","CANCELADA","OUTRO"]

def map_legacy_status(code: Optional[str]) -> StatusOP:
    c = (code or "").strip().upper()
    if c == "AA": return "ABERTA"
    if c in ("SS"): return "ENTRADA_PARCIAL"
    if c == "FF": return "FINALIZADA"
    if c == "CC": return "CANCELADA"
    return "OUTRO"

# Mapa de código numérico do setor -> nome usado no app (igual ao setores.map.ts)
SETOR_LEGACY_MAP: Dict[int,str] = {
    1: "Perfiladeira",
    3: "Serralheria",
    4: "Pintura",
    6: "Eixo",
    # quando souber: <codigo>: "Expedição",
}
