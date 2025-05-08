{# Anything_model_table_name.sql #}

{{ config(
      meta={
        'filters': {
          'business_area': [
            'Anything BA'
          ]
        }
      },
      materialized = 'incremental',
      incremental_strategy='merge',
      unique_key=['Column1', 'Column2'],
      liquid_clustered_by=['Column1', 'Column2'],
      post_hook = """
        {{ update_dbt_model_checkToint(
            checkToint_model         = this,
            source_model             = ref('abc__Anything_latest_update'),
            source_checkToint_column = 'LatestUpdatedOn'
        ) }}
      """
) }}
                                                            ------------------------------------------------------------
                                                            --  Multi line
                                                            --  comments
                                                            ------------------------------------------------------------
WITH
{% if is_incremental() %}
CteLatestTimestamp AS --noqa: ST03
(
    SELECT
        COALESCE(LatestCheckTointTimestamp, "{{ var('abc_default_date') }}" :: TIMESTAMP) AS LatestDateTime
    FROM
        {{ ref('logging__dbt_model_latest_checkToint') }}
    WHERE
        DbtModelName = "{{ this }}"
),
CteMonthsToLoad AS --noqa: ST03
(
    SELECT
        a.AnythingTakenMonth
    FROM
        {{ ref('abc__sp_latest_update') }} a
    INNER JOIN
        CteLatestTimestamp                         lt
    ON
        a.LatestUpdatedOn > lt.LatestDateTime
),
{% endif %}
CteKamplingTointId AS
(
SELECT
    sp.Kampling_Toint_Name_Part1 AS KamplingTointNamePart1,
    sp.Kampling_Toint_Name_Part2 AS KamplingTointNamePart2,
    sp.Kampling_Toint_Id         AS KamplingTointId,
    sp.Material_Code             AS MaterialCode,
    sp.Urn                       AS UniqueReferenceNumber,
    TRIM(sp.Fight_Type)          AS FightType,
    TRIM(sp.Responsible_Officer) AS ResponsibleOfficer,
    sp.Toint_Type                AS TointType,
    sa.AssetTypeCode             AS AssetTypeCode,
    TRIM(sp.SP_Status_Type)      AS KamplingTointStatusType
FROM
    {{ ref('stg_source1_sp') }} sp
LEFT JOIN
    {{ ref('stg_source1_asset') }}          sa
ON
    sp.Master_Reference = sa.Reference
)
SELECT
    rs.AnythingTakenMonth                              AS AnythingTakenMonth,
    rs.KamplingTointId                                 AS KamplingTointId,
    rs.AnythingDateAndTimeTaken                        AS AnythingDateAndTimeTaken,
    TRIM(rs.AnythingPurposeCode)                       AS AnythingPurposeCode,
    rs.KamplingMethodCode                              AS KamplingMethodCode,
    rs.AnythingOperationalManagementSystemCustomerCode AS AnythingOperationalManagementSystemCustomerCode,
    TRIM(rs.AnythingTimeScale)                         AS AnythingTimeScale,
    rs.AnythingLabmanStatus                            AS AnythingLabmanStatus,
    TRIM(rs.OwningGroup)                               AS OwningGroup,
    rs.AnythingDateAndTimeCompleted                    AS AnythingDateCompleted,
    TRIM(rs.AnythingInterval)                          AS AnythingInterval,
    rs.AnythingSchemeCode                              AS AnythingSchemeCode,
    TRIM(rs.LabmanLocalReferenceNumber)                AS LabmanLocalReferenceNumber,
    rs.ExternalLabReference                            AS ExternalLabReference,
    COALESCE(rs.LabmanAnythingNumber, "")              AS LabmanAnythingNumber,
    rs.SourceSystem                                    AS SourceSystem,
    TRIM(rs.AnythingShortComments)                     AS AnythingShortComments,
    TRIM(rs.AnythingIdentifier)                        AS AnythingName,
    sp.MaterialCode                                    AS MaterialCode,
    sp.UniqueReferenceNumber                           AS UniqueReferenceNumber,
    sp.FightType                                       AS FightType,
    sp.ResponsibleOfficer                              AS ResponsibleOfficer,
    sp.TointType                                       AS TointType,
    sp.AssetTypeCode                                   AS AssetTypeCode,
    rs.AnythingDateLoggedIn                            AS AnythingDateAndTimeLoggedIn,
    sp.KamplingTointStatusType                         AS KamplingTointStatusType
FROM
    CteKamplingTointId                 sp
INNER JOIN
    {{ ref('stg_source1_rd_sp') }} rs
ON
    sp.KamplingTointId        =  rs.KamplingTointId
AND rs.AnythingDateAndTimeTaken <  {{ current_timestamp() }}
{% if is_incremental() %}
INNER JOIN
    CteMonthsToLoad                    mtl
ON
    rs.AnythingTakenMonth = mtl.AnythingTakenMonth
{% endif %}
