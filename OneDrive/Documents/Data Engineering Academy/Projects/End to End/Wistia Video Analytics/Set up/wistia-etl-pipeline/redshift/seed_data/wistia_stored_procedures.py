###Script to create stored procedures 
 ########################## dim_media #####################################
CREATE OR REPLACE PROCEDURE sp_upsert_dim_media()
LANGUAGE plpgsql
AS $$
BEGIN

    -- Step 1: Remove existing rows with same media_id
    DELETE FROM public.dim_media
    USING public.dim_media_stage
    WHERE dim_media.media_id = dim_media_stage.media_id;

    -- Step 2: Insert fresh rows from stage
    INSERT INTO public.dim_media
    (
        media_id,
        media_name,
        duration_seconds,
        created_at,
        updated_at,
        section_name,
        subfolder_name,
        thumbnail_url,
        project_name
    )
    SELECT
        media_id,
        media_name,
        duration_seconds,
        created_at,
        updated_at,
        section_name,
        subfolder_name,
        thumbnail_url,
        project_name
    FROM public.dim_media_stage;

    -- Step 3: Clear stage
    TRUNCATE public.dim_media_stage;

END;
$$;




--- ########################## media_daily_agg #####################################

CREATE OR REPLACE PROCEDURE sp_upsert_media_daily_agg()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Remove existing rows with same media_id + dt
    DELETE FROM public.media_daily_agg
    USING public.media_daily_agg_stage
    WHERE media_daily_agg.media_id = media_daily_agg_stage.media_id
      AND media_daily_agg.dt = media_daily_agg_stage.dt;

    -- Step 2: Insert fresh rows from stage
    INSERT INTO public.media_daily_agg
    (
        media_id,
        dt,
        load_count,
        play_count,
        play_rate,
        hours_watched,
        engagement,
        visitors
    )
    SELECT
        media_id,
        dt,
        load_count,
        play_count,
        play_rate,
        hours_watched,
        engagement,
        visitors
    FROM public.media_daily_agg_stage;

    -- Step 3: Clear stage
    TRUNCATE public.media_daily_agg_stage;
END;
$$;


