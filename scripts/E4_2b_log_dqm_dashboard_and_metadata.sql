-- SCRIPT: log_dqm_dashboard_publication.sql
DO $$
DECLARE
    v_script_nombre TEXT := 'E4_12b_log_dqm_dashboard_publication.sql';
    v_script_desc   TEXT := 'Registra la publicación del tablero de Monitoreo de Calidad de Datos (DQM) en Power BI.';
    v_created_by    TEXT := 'Agustina';
    v_log_id BIGINT; v_script_id INT;
BEGIN
    SELECT script_id INTO v_script_id FROM dqm_scripts_inventory WHERE script_name = v_script_nombre;
    IF v_script_id IS NULL THEN
        INSERT INTO dqm_scripts_inventory (script_name, description, created_by) VALUES (v_script_nombre, v_script_desc, v_created_by) RETURNING script_id INTO v_script_id;
    END IF;
    INSERT INTO dqm_exec_log (script_id, started_at, status, finished_at, message) 
    VALUES (v_script_id, NOW(), 'OK', NOW(), 'Tablero de Monitoreo de Calidad de Datos (DQM) publicado en Power BI.');
END $$;

-- SCRIPT: document_dqm_dashboard_in_metadata.sql
INSERT INTO md_entities (
    entity_name, 
    business_name, 
    layer, 
    entity_type, 
    description, 
    created_by,
    is_active
) VALUES (
    'TBD_01_Monitoreo_DQM', 
    'Tablero de Monitoreo de Calidad de Datos (DQM)',
    'Gobernanza',
    'Dashboard',
    'Tablero en Power BI para el monitoreo de la salud de los procesos ETL y la calidad de los datos del DWA. Fuentes: dqm_exec_log, dqm_resultados_calidad, etc. URL: https://app.powerbi.com/groups/me/reports/fa35ff78-574c-4012-873f-0ab40f3aa770/a3302d7f0566b63eb217?experience=power-bi',
    'Agustina',
    TRUE
)
ON CONFLICT (entity_name) DO NOTHING;
