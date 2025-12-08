-- Создание таблицы processing_status
CREATE TABLE public.processing_status (
    status_id       SMALLINT PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    description     TEXT NOT NULL
);

-- Заполнение таблицы processing_status
insert into public.processing_status (status_id, code, description) values
(1, 'file_detected', 'Обнаружен обновленный файл'),
(2,'file_load_to_store', 'Файл загружается в хранилище'),
(3,'reading_file', 'Чтение файла и запись в базу данных'),
(4,'stage_db_load_completed', 'Загрузка файла в stage базу данных'),
(5,'sandbox_db_load', 'Загрузка файла в sandbox базу данных'),
(6,'processing_completed', 'Обработка файла завершена'),
(7,'processing_failed', 'Обработка файла завершена с ошибкой');

-- Создание таблицы processing_error
CREATE TABLE public.processing_error (
    error_id        SMALLINT PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    description     TEXT NOT null,
    error_level     SMALLINT
);

-- Заполнение таблицы processing_error
insert into public.processing_error (error_id, code, description, error_level) values
(1, 'extension_error', 'Ошибка расширения', 2),
(2,'merged_cells_detected', 'Обнаружены объединенные ячейки на 1м листе', 2),
(3,'more_than_one_list_detected', 'Файл содержит более 1 листа', 1),
(4,'s3_connection_failed', 'Ошибка подключения к S3 хранилищу', 2);

-- Создание таблицы buckets
CREATE TABLE public.buckets (
    bucket_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_name     TEXT NOT NULL UNIQUE,
    description     TEXT DEFAULT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- Заполнение таблицы buckets
insert into public.buckets (bucket_name, description) values 
('commercialbucket', 'Бакет коммерческого блока'),
('medicalbucket', 'Бакет медицинского блока'),
('hrbucket', 'Бакет кадравого блока'),
('accountingbucket', 'Бакет бухгалтерского блока'),
('financialbucket', 'Бакет финансового блока');

-- Создание таблицы file_metadata
CREATE TABLE IF NOT EXISTS public.file_metadata (
    file_id TEXT,
    file_name TEXT,
    user_creator_name TEXT,
    last_modified TIMESTAMP NOT NULL,
    size BIGINT,
    bucket_id UUID NOT NULL,
    file_load_status_id SMALLINT NOT NULL,
    error_id SMALLINT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    key VARCHAR(255),
    CONSTRAINT fk_file_bucket 
        FOREIGN KEY (bucket_id) 
        REFERENCES buckets(bucket_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,            
    CONSTRAINT fk_file_status 
        FOREIGN KEY (file_load_status_id) 
        REFERENCES processing_status(status_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,            
    CONSTRAINT fk_file_error 
        FOREIGN KEY (error_id) 
        REFERENCES processing_error(error_id)
        ON DELETE SET NULL
        ON UPDATE cascade
);

-- Создание таблицы file_metadata_history
CREATE TABLE IF NOT EXISTS public.file_metadata_history (
    file_id TEXT,
    file_name TEXT,
    user_creator_name TEXT,
    last_modified TIMESTAMP NOT NULL,
    size BIGINT,
    bucket_id UUID NOT NULL,
    file_load_status_id SMALLINT NOT NULL,
    error_id SMALLINT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    key VARCHAR(255),
    CONSTRAINT fk_file_bucket 
        FOREIGN KEY (bucket_id) 
        REFERENCES buckets(bucket_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,    
    CONSTRAINT fk_file_status 
        FOREIGN KEY (file_load_status_id) 
        REFERENCES processing_status(status_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,            
    CONSTRAINT fk_file_error 
        FOREIGN KEY (error_id) 
        REFERENCES processing_error(error_id)
        ON DELETE SET NULL
        ON UPDATE cascade
);

-- Создание функции log_file_metadata_change
CREATE OR REPLACE FUNCTION log_file_metadata_change()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO public.file_metadata_history (
        file_id,
        file_name,
        user_creator_name,
        last_modified,
        size,
        bucket_id,
        file_load_status_id,
        error_id,
        created_at,
        updated_at,
        key
    )
    VALUES (
        NEW.file_id,
        NEW.file_name,
        NEW.user_creator_name,
        NEW.last_modified,
        NEW.size,
        NEW.bucket_id,
        NEW.file_load_status_id,
        NEW.error_id,
        NEW.created_at,
        NEW.updated_at,
        NEW.key
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Индекс для ускорения поиска по ключу объекта
CREATE INDEX IF NOT EXISTS idx_file_metadata_key ON public.file_metadata(key);

-- Функция возвращает записи из file_metadata, которых нет в текущем списке ключей S3
CREATE OR REPLACE FUNCTION public.get_missing_s3_files(current_keys text[])
RETURNS TABLE (
    key text,
    file_name text,
    user_name text,
    guid text,
    bucket text,
    table_name text
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Если current_keys пуст или null, возвращаем все файлы из метаданных
    IF current_keys IS NULL OR array_length(current_keys, 1) IS NULL OR array_length(current_keys, 1) = 0 THEN
        RETURN QUERY
        WITH meta AS (
            SELECT DISTINCT ON (fm.key)
                fm.key::text,
                fm.file_name::text,
                fm.user_creator_name::text AS user_name,
                fm.file_id::text AS guid,
                b.bucket_name::text AS bucket
        FROM public.file_metadata fm
        JOIN public.buckets b ON fm.bucket_id = b.bucket_id
        WHERE fm.key IS NOT NULL
        )
        SELECT m.key,
            m.file_name,
            m.user_name,
            m.guid,
            m.bucket,
            replace(replace(m.file_name, ' ', '_'), '.', '_')::text AS table_name
        FROM meta m;
    ELSE
        -- Если есть ключи, возвращаем только те, которых нет в S3
        RETURN QUERY
        WITH s3_now AS (
            SELECT unnest(current_keys) AS key
        ),
        meta AS (
            SELECT DISTINCT ON (fm.key)
                fm.key::text,
                fm.file_name::text,
                fm.user_creator_name::text AS user_name,
                fm.file_id::text AS guid,
                b.bucket_name::text AS bucket
        FROM public.file_metadata fm
        JOIN public.buckets b ON fm.bucket_id = b.bucket_id
        WHERE fm.key IS NOT NULL
        )
        SELECT m.key,
            m.file_name,
            m.user_name,
            m.guid,
            m.bucket,
            replace(replace(m.file_name, ' ', '_'), '.', '_')::text AS table_name
        FROM meta m
        LEFT JOIN s3_now s ON s.key::text = m.key
        WHERE s.key IS NULL;
    END IF;
END;
$$;

-- Создание триггера trg_file_metadata_history
CREATE TRIGGER trg_file_metadata_history
AFTER INSERT OR UPDATE ON public.file_metadata
FOR EACH ROW
EXECUTE FUNCTION log_file_metadata_change();

-- Создание функции check_update_file_s3
CREATE OR REPLACE FUNCTION check_update_file_s3(
    p_file_name VARCHAR(255), 
    p_user_name VARCHAR(255), 
    p_guid VARCHAR(255),
    p_bucket VARCHAR(255),
    p_last_modified TIMESTAMP, 
    p_size BIGINT,
    p_key VARCHAR(255)
)
RETURNS smallint AS $$
DECLARE
    p_bucket_id UUID;
    p_file_load_status_id SMALLINT := 1;
    p_error_id SMALLINT := NULL;
    p_created_at TIMESTAMP := NOW();
    p_updated_at TIMESTAMP := NULL;
BEGIN
    -- Получаем bucket_id, если бакет существует
    SELECT bucket_id FROM public.buckets WHERE bucket_name = p_bucket INTO p_bucket_id;
    
    -- Если бакет не найден, создаём его автоматически
    IF p_bucket_id IS NULL THEN
        INSERT INTO public.buckets (bucket_name, description)
        VALUES (p_bucket, 'Автоматически созданный бакет')
        ON CONFLICT (bucket_name) DO NOTHING
        RETURNING bucket_id INTO p_bucket_id;
        
        -- Если всё ещё NULL (конфликт), получаем существующий
        IF p_bucket_id IS NULL THEN
            SELECT bucket_id FROM public.buckets WHERE bucket_name = p_bucket INTO p_bucket_id;
        END IF;
    END IF;
    
    SELECT status_id FROM public.processing_status WHERE code = 'file_detected' INTO p_file_load_status_id;
    IF NOT EXISTS (SELECT 1 FROM public.file_metadata as fm inner join public.buckets as b on fm.bucket_id = b.bucket_id
                            WHERE fm.file_name = p_file_name
                                and b.bucket_name = p_bucket
                    ) THEN
        
        INSERT INTO public.file_metadata (
            file_id,
            file_name,
            user_creator_name,
            last_modified,
            size,
            bucket_id,
            file_load_status_id,
            error_id,
            created_at,
            updated_at,
            key
        )
        VALUES (
            p_guid, 
            p_file_name, 
            p_user_name, 
            p_last_modified, 
            p_size,
            p_bucket_id, 
            p_file_load_status_id, 
            NULL, 
            p_created_at, 
            NULL,
            p_key
        );
        RETURN 1;
    ELSIF EXISTS (SELECT 1 FROM public.file_metadata as fm inner join public.buckets as b on fm.bucket_id = b.bucket_id
                            WHERE fm.file_name = p_file_name
                                and b.bucket_name = p_bucket
                                and (fm.last_modified != p_last_modified or fm.size != p_size)
                                and not exists (select 1 from public.file_metadata as f where f.key=p_key)) THEN
            
        INSERT INTO public.file_metadata (
            file_id,
            file_name,
            user_creator_name,
            last_modified,
            size,
            bucket_id,
            file_load_status_id,
            error_id,
            created_at,
            updated_at,
            key
        )
        VALUES (
            p_guid, 
            p_file_name, 
            p_user_name, 
            p_last_modified, 
            p_size,
            p_bucket_id, 
            p_file_load_status_id, 
            NULL, 
            p_created_at, 
            NULL,
            p_key
        );
        RETURN 2;
    ELSIF p_file_name NOT LIKE '%.xlsx' THEN
        SELECT status_id FROM public.processing_status WHERE code = 'processing_failed' INTO p_file_load_status_id;
        SELECT error_id FROM public.processing_error WHERE code = 'extension_error' INTO p_error_id;
            INSERT INTO public.file_metadata (
                file_id,
                file_name,
                user_creator_name,
                last_modified,
                size,
                bucket_id,
                file_load_status_id,
                error_id,
                created_at,
                updated_at,
                key
            )
            VALUES (
                p_guid, 
                p_file_name, 
                p_user_name, 
                p_last_modified, 
                p_size,
                p_bucket_id, 
                p_file_load_status_id, 
                p_error_id, 
                p_created_at, 
                p_updated_at,
                p_key
            )
            ON CONFLICT(file_name, bucket_id) 
            DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size, user_creator_name = EXCLUDED.user_creator_name, last_modified = EXCLUDED.last_modified, updated_at = NOW(), check_actual_file=true;
            RETURN 3;
    ELSE
        RETURN 4;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Создание функции update_status_file_s3
CREATE OR REPLACE FUNCTION update_status_file_s3( 
    p_guid VARCHAR(255),
    p_status_code VARCHAR(255)
)    
RETURNS bool AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM public.file_metadata as fm where fm.file_id=p_guid) THEN 
        IF p_status_code='extension_error' THEN
            UPDATE public.file_metadata
            SET error_id=1,
                file_load_status_id=7,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN FALSE;
        ELSIF p_status_code='merged_cells_detected' THEN
            UPDATE public.file_metadata
            SET error_id=2,
                file_load_status_id=7,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN FALSE;
        ELSIF p_status_code='more_than_one_list_detected' THEN
            UPDATE public.file_metadata
            SET error_id=3,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN TRUE;
        ELSIF p_status_code='s3_connection_failed' THEN
            UPDATE public.file_metadata
            SET error_id=4,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN FALSE;
        ELSIF p_status_code='file_load_to_store' THEN
            UPDATE public.file_metadata
            SET file_load_status_id=2,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN TRUE;
        ELSIF p_status_code='reading_file' THEN
            UPDATE public.file_metadata
            SET file_load_status_id=3,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN TRUE;
        ELSIF p_status_code='stage_db_load_completed' THEN
            UPDATE public.file_metadata
            SET file_load_status_id=4,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN TRUE;
        ELSIF p_status_code='sandbox_db_load' THEN
            UPDATE public.file_metadata
            SET file_load_status_id=5,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN TRUE;
        ELSIF p_status_code='processing_completed' THEN
            UPDATE public.file_metadata
            SET file_load_status_id=6,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN TRUE;
        ELSIF p_status_code='processing_failed' THEN
            UPDATE public.file_metadata
            SET file_load_status_id=7,
                updated_at = NOW()
            WHERE file_id=p_guid;
            RETURN FALSE;
        END IF;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql;