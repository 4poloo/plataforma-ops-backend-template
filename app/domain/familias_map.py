# app/domain/familias_map.py
# Mapa maestro familia/subfamilia <-> códigos.
# Mantén estos datos en un solo lugar para que Import/Crear/Actualizar lo usen igual.

from typing import Optional, Tuple

# 👇 Ejemplo: ajústalo a tu catálogo real
FAMILIAS = {
    "AUTOMOTRIZ": {
    "codigo": 1,
    "subs": {
      "REFRIGERANTES": 1,
      "CARCARE": 2,
      "LUBRICANTES": 3,
    },
  },
  "LIMPIEZA": {
    "codigo": 2,
    "subs": {
      "DETERGENTES": 4,
      "LIMPIADORES": 5,
      "AROMATIZANTES": 6,
      "OTROS": 7,
    },
  },
  "MAQUINARIA": {
    "codigo": 3,
    "subs": {
      "JARDINERIA": 8,
      "INDUSTRIAL": 9,
    },
  },
  "ELABORACION": {
    "codigo": 4,
    "subs": {
      "AUTOMOTRIZ": 10,
      "LIMPIEZA": 11,
    },
  },
  "INSUMOS": {
    "codigo": 5,
    "subs": {
      "ENVASES": 12,
      "CAJAS": 13,
      "ETIQUETAS": 14,
      "CONSUMIBLES": 15,
      "QUIMICOS": 16,
    },
  },
  "FIESTA": {
    "codigo": 6,
    "subs": {
        "FIESTA": 17
    },
  },
}

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def resolve_codes(
    dg: Optional[str],               # nombre familia
    dsg: Optional[str],              # nombre subfamilia
    codigo_g: Optional[int],         # código familia
    codigo_sg: Optional[int],        # código subfamilia
) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
    """
    Retorna (codigo_g_res, codigo_sg_res, dg_res, dsg_res)
    - Si viene nombre, lo usamos para buscar códigos.
    - Si viene código, devolvemos también el nombre canónico.
    - Tolera que venga mezcla (ej. nombre familia + código subfamilia).
    """
    dg_n = _norm(dg)
    dsg_n = _norm(dsg)

    # 1) Si tenemos nombre de familia, prioriza el nombre para definir codigo_g
    cod_g = codigo_g
    fam_name = dg
    if dg_n:
        if dg_n in FAMILIAS:
            cod_g = FAMILIAS[dg_n]["codigo"]
            fam_name = dg_n.title()
        # si no está, deja lo que venía (podrías avisar más adelante)

    # 2) Si no había nombre pero sí código, infiere el nombre
    if not dg_n and codigo_g is not None:
        for name, data in FAMILIAS.items():
            if data["codigo"] == codigo_g:
                fam_name = name.title()
                break

    # 3) Subfamilia: usa nombre si existe y tenemos familia resuelta
    cod_sg = codigo_sg
    sub_name = dsg
    fam_key = _norm(fam_name)
    if dsg_n and fam_key in FAMILIAS:
        cod_sg = FAMILIAS[fam_key]["subs"].get(dsg_n)
        sub_name = dsg_n.title()

    # 4) Si no hay nombre pero sí código de subfamilia, intenta inferirlo buscando en familia
    if not dsg_n and cod_sg is not None and fam_key in FAMILIAS:
        for s_name, s_code in FAMILIAS[fam_key]["subs"].items():
            if s_code == cod_sg:
                sub_name = s_name.title()
                break

    return cod_g, cod_sg, fam_name, sub_name
