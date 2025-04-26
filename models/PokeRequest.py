from pydantic import BaseModel, Field
from typing import Optional

class PokemonRequest(BaseModel):

    id: Optional[int] = Field(
        default=None,
        ge=1,
        description="ID de la peticion"
    )

    pokemon_type: Optional[str] = Field(
        default=None,
        description="Tipo de pokemon",
        pattern="^[a-zA-Z0-9_]+$"
    )

    url : Optional[str] = Field(
        default=None,
        description="URL de la peticion",
        pattern="^https?://[^\s]+$"
    )

    status: Optional[str] = Field(
        default=None,
        description="Estado de la peticion",
        pattern="^(sent|completed|failed|inprogress)$"
    )