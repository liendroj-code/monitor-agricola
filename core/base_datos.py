"""
Módulo de base de datos interconectando capa Supabase
"""

import streamlit as st
from st_supabase_connection import SupabaseConnection

def get_supabase():
    """Obtiene el cliente de Supabase desde el wrapper de Streamlit"""
    return st.connection("supabase", type=SupabaseConnection)

def get_session_id():
    """Devuelve el ID UUID del usuario autenticado de la sesión"""
    if "user_id" in st.session_state:
        return st.session_state.user_id
    # En esta versión con Supabase, siempre debemos tener user_id validado.
    raise ValueError("Usuario no validado/ausente. Por favor, inicie sesión.")

def inicializar():
    """Supabase administra tablas desde su lado. Compatible con app.py"""
    if "db_initialized" not in st.session_state:
        st.session_state.db_initialized = True
        print(f"Iniciando instancia Supabase para el usuario: {st.session_state.get('user_name', 'Desconocido')}")

# ============================================================
# FUNCIONES PARA LOTES (CRUD SUPABASE)
# ============================================================

def guardar_lote(datos):
    """Inserta un nuevo lote en la tabla 'lotes' asociado al usuario activo"""
    supabase = get_supabase()
    user_uid = get_session_id()
    
    data_to_insert = {
        "user_id": user_uid,
        "campana": datos['campana'],
        "establecimiento": datos['establecimiento'],
        "lote": datos['lote'],
        "localidad": datos['localidad'],
        "provincia": datos.get('provincia', ''),
        "lat": datos['lat'],
        "lon": datos['lon'],
        "cultivo": datos['cultivo'],
        "variedad": datos['variedad'],
        "fecha_siembra": datos['fecha_siembra'],
        "rinde_potencial": datos['rinde_potencial']
    }
    
    res = supabase.table("lotes").insert(data_to_insert).execute()
    # supabase python client insert() -> devuelve data=[] en res.data
    lote_id = res.data[0]['id'] if res.data else None
    return lote_id

def listar_lotes():
    """Lista todos los lotes correspondientes al usuario de la sesión actual (RLS previene cruces)"""
    supabase = get_supabase()
    user_uid = get_session_id()
    
    res = supabase.table("lotes").select("*").eq("user_id", user_uid).order("id", desc=True).execute()
    return res.data

def actualizar_lote(lote_id, datos):
    """Actualiza un lote existente con Supabase"""
    supabase = get_supabase()
    user_uid = get_session_id()
    
    data_to_update = {
        "campana": datos['campana'],
        "establecimiento": datos['establecimiento'],
        "lote": datos['lote'],
        "localidad": datos['localidad'],
        "provincia": datos.get('provincia', ''),
        "lat": datos['lat'],
        "lon": datos['lon'],
        "cultivo": datos['cultivo'],
        "variedad": datos['variedad'],
        "fecha_siembra": datos['fecha_siembra'],
        "rinde_potencial": datos['rinde_potencial']
    }
    
    supabase.table("lotes").update(data_to_update).eq("id", lote_id).eq("user_id", user_uid).execute()

def eliminar_lote(lote_id):
    """Aprovechando ON DELETE CASCADE en supabase los monitoreos volaran si los borramos."""
    supabase = get_supabase()
    user_uid = get_session_id()
    
    supabase.table("lotes").delete().eq("id", lote_id).eq("user_id", user_uid).execute()

# ============================================================
# FUNCIONES PARA MONITOREOS (CRUD SUPABASE)
# ============================================================

def guardar_monitoreo(datos):
    """Guarda un nuevo monitoreo. Recordar setear context de user_id."""
    supabase = get_supabase()
    user_uid = get_session_id()
    
    datos["user_id"] = user_uid
    res = supabase.table("monitoreos").insert(datos).execute()
    mon_id = res.data[0]['id'] if res.data else None
    return mon_id

def listar_monitoreos(lote_id=None, etapa=None, fecha_desde=None, fecha_hasta=None):
    """Lista monitoreos con Supabase filtering"""
    supabase = get_supabase()
    user_uid = get_session_id()
    
    query = supabase.table("monitoreos").select("*").eq("user_id", user_uid)
    
    if lote_id:
        query = query.eq("lote_id", lote_id)
    if etapa and etapa != 'Todas':
        query = query.eq("etapa_fenologica", etapa)
    if fecha_desde:
        query = query.gte("fecha", fecha_desde)
    if fecha_hasta:
        query = query.lte("fecha", fecha_hasta)
        
    query = query.order("fecha", desc=True)
    res = query.execute()
    return res.data

def eliminar_monitoreo(monitoreo_id):
    """Elimina un monitoreo de la nube Supabase"""
    supabase = get_supabase()
    user_uid = get_session_id()
    
    supabase.table("monitoreos").delete().eq("id", monitoreo_id).eq("user_id", user_uid).execute()
