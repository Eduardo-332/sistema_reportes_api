import json 
import logging
from fastapi import HTTPException
from models.PokeRequest import PokemonRequest
from utils.database import execute_query_json
from utils.AQueue import AQueue
from utils.ABlob import ABlob


# configurar el logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def select_pokemon_request( id: int ):
    try:
        query = "select * from pokequeue.requests where id = ?"
        params = (id,)
        result = await execute_query_json( query , params )
        result_dict = json.loads(result)
        return result_dict
    except Exception as e:
        logger.error( f"Error selecting report request {e}" )
        raise HTTPException( status_code=500 , detail="Internal Server Error" )


async def update_pokemon_request( pokemon_request: PokemonRequest) -> dict:
    try:
        query = " exec pokequeue.update_poke_request ?, ?, ? "
        if not pokemon_request.url:
            pokemon_request.url = "";

        params = ( pokemon_request.id, pokemon_request.status, pokemon_request.url, )
        result = await execute_query_json( query , params, True )
        result_dict = json.loads(result)
        return result_dict
    except Exception as e:
        logger.error( f"Error updating report request {e}" )
        raise HTTPException( status_code=500 , detail="Internal Server Error" )


async def insert_pokemon_request( pokemon_request: PokemonRequest) -> dict:
    try:
        query = " exec pokequeue.create_poke_request ?, ? "
        params = ( pokemon_request.pokemon_type, pokemon_request.sample_size or None, )
        result = await execute_query_json( query , params, True )
        result_dict = json.loads(result)

        await AQueue().insert_message_on_queue( result )

        return result_dict
    except Exception as e:
        logger.error( f"Error inserting report reques {e}" )
        raise HTTPException( status_code=500 , detail="Internal Server Error" )


async def get_all_request() -> dict:
    query = """
        select 
            r.id as ReportId
            , s.description as Status
            , r.type as PokemonType
            , r.url 
            , r.created 
            , r.updated
        from pokequeue.requests r 
        inner join pokequeue.status s 
        on r.id_status = s.id 
    """
    result = await execute_query_json( query  )
    result_dict = json.loads(result)
    blob = ABlob()
    for record in result_dict:
        id = record['ReportId']
        record['url'] = f"{record['url']}?{blob.generate_sas(id)}"
    return result_dict


async def delete_pokemon_request(report_id: int) -> dict:
    try:
        # 1. Verificar que el reporte existe
        query = "SELECT id, type FROM pokequeue.requests WHERE id = ?"
        params = (report_id,)
        result = await execute_query_json(query, params)
        result_dict = json.loads(result)
        
        if not result_dict:
            logger.error(f"Report with ID {report_id} not found")
            raise HTTPException(status_code=404, detail=f"Reporte con ID {report_id} no encontrado")
            
        blob_success = False
        try:
            blob_client = ABlob()
            blob_success = blob_client.delete_blob(report_id)
            
            if blob_success:
                logger.info(f"Blob para el reporte {report_id} eliminado correctamente")
            else:
                logger.warning(f"No se encontró el blob para el reporte {report_id}")
            
        except Exception as e:
            logger.error(f"Error al eliminar blob: {str(e)}")
        
        delete_query = "DELETE FROM pokequeue.requests WHERE id = ?"
        await execute_query_json(delete_query, params, True)
        
        if blob_success:
            return {"message": f"Reporte con ID {report_id} y su archivo eliminados correctamente"}
        else:
            return {"message": f"Reporte con ID {report_id} eliminado de la base de datos. No se encontró archivo asociado en Blob."}
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error al eliminar solicitud de reporte: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")