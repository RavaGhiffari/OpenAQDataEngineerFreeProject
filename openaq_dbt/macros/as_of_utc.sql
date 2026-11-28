{#
  Waktu acuan (UTC, TIMESTAMP tanpa zona) untuk logika relatif seperti is_active.
  Airflow mengirim --vars '{"as_of_utc": "<data_interval_end>"}' sehingga hasilnya hanya
  bergantung pada run, bukan pada kapan dbt kebetulan dijalankan (penting untuk backfill/rerun).
  Tanpa var (dbt lokal), fallback ke now().
#}
{% macro as_of_utc() -%}
    {%- set as_of = var('as_of_utc', none) -%}
    {%- if as_of -%}
        (cast('{{ as_of }}' as timestamptz) at time zone 'UTC')
    {%- else -%}
        (now() at time zone 'UTC')
    {%- endif -%}
{%- endmacro %}
