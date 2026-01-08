from datetime import date, datetime, timedelta
from typing import Union, Tuple


DateLike = Union[date, datetime]


def _sinan_epiweek_info(d: DateLike) -> Tuple[int, int, date, date]:
    """
    Retorna (epi_year, epiweek_num, week_start_sun, week_end_sat) seguindo SINAN:
    - semana: domingo -> sábado
    - ano epidemiológico: ano que tem a maioria dos dias da semana
      (equivalente ao ano da quarta-feira dessa semana)
    - semana 1: semana (dom->sáb) que contém 4 de janeiro
    """
    base_date = d.date() if isinstance(d, datetime) else d

    # Início da semana (domingo)
    days_since_sunday = (base_date.weekday() + 1) % 7  # Mon=0..Sun=6 => Sun vira 0
    week_start = base_date - timedelta(days=days_since_sunday)
    week_end = week_start + timedelta(days=6)

    # Ano epidemiológico = ano da quarta-feira (dia "do meio" da semana dom-sáb)
    epi_year = (week_start + timedelta(days=3)).year

    # Semana 1 do epi_year = semana que contém 4 de janeiro
    jan4 = date(epi_year, 1, 4)
    week1_start = jan4 - timedelta(days=((jan4.weekday() + 1) % 7))

    epiweek_num = ((week_start - week1_start).days // 7) + 1
    return epi_year, epiweek_num, week_start, week_end


def get_epiweek_str(
    datetime_: DateLike | None = None,
    format: str = "{EPINUM}",
    zfill: int = 0,
    use_epiweek_enddate: bool = True,
) -> str:
    """
    Cria uma string com a semana epidemiológica (SINAN).

    Placeholders suportados:
      - {EPINUM}: número da semana epidemiológica (1-52/53)
      - {EPIYEAR}: ano epidemiológico (regra da maioria dos dias)
      - {EPISTART}: início da semana (domingo) em ISO (YYYY-MM-DD)
      - {EPIEND}: fim da semana (sábado) em ISO (YYYY-MM-DD)

    O restante do `format` é tratado por `strftime` normalmente.

    Nota sobre `zfill`:
      - aplica padding APENAS ao {EPINUM}.
    """
    if datetime_ is None:
        datetime_ = datetime.now()

    if not isinstance(datetime_, (datetime, date)):
        raise TypeError(f"'datetime_' deve ser datetime ou date, não {type(datetime_)}.")
    if not isinstance(format, str):
        raise TypeError(f"'format' deve ser str, não {type(format)}.")
    if "{EPINUM}" not in format:
        raise ValueError("'format' deve conter '{EPINUM}'.")
    if not isinstance(zfill, int):
        raise TypeError(f"'zfill' deve ser int, não {type(zfill)}.")
    if zfill < 0:
        raise ValueError("'zfill' deve ser >= 0.")
    if not isinstance(use_epiweek_enddate, bool):
        raise TypeError(f"'use_epiweek_enddate' deve ser bool, não {type(use_epiweek_enddate)}.")

    epi_year, epiweek_num, week_start, week_end = _sinan_epiweek_info(datetime_)

    # Data âncora para o strftime (mantém compatibilidade com sua ideia de "enddate")
    anchor = datetime_
    if use_epiweek_enddate:
        if isinstance(datetime_, datetime):
            # mantém hora/tzinfo e troca só a data
            anchor = datetime_.replace(year=week_end.year, month=week_end.month, day=week_end.day)
        else:
            anchor = week_end

    base_str = anchor.strftime(format)

    replacements = {
        "{EPINUM}": str(epiweek_num).zfill(zfill),
        "{EPIYEAR}": str(epi_year),
        "{EPISTART}": week_start.isoformat(),
        "{EPIEND}": week_end.isoformat(),
    }

    for k, v in replacements.items():
        base_str = base_str.replace(k, v)

    return base_str
