CREATE TABLE dbo.analisis_resultado (
    entity VARCHAR(255),        -- Almacena el tipo de entidad, como 'Dataset', 'Column', etc.
    instance VARCHAR(255),      -- Almacena la instancia, como '*', 'AddressLine2', 'PostalCode', etc.
    name VARCHAR(255),          -- Almacena el nombre del análisis, como 'Size', 'Completeness', etc.
    value FLOAT,                -- Almacena el valor numérico del análisis, como '450.0', '0.024444', etc.
    porcentaje VARCHAR(10)      -- Almacena el porcentaje como string, como '45000%' o '2%'
);

CREATE TABLE dbo.check_resultado (
    entity VARCHAR(255),        -- Almacena el tipo de entidad, como 'Dataset', 'Column', etc.
    instance VARCHAR(255),      -- Almacena la instancia, como '*', 'AddressLine2', 'PostalCode', etc.
    name VARCHAR(255),          -- Almacena el nombre del check, como 'Size', 'Completeness', etc.
    value FLOAT,                -- Almacena el valor numérico del check, como '450.0', '0.024444', etc.
    porcentaje VARCHAR(10)      -- Almacena el porcentaje como string, como '45000%' o '2%'
);

ALTER TABLE dbo.analisis_resultado
ADD fechaHora DATETIME;

ALTER TABLE dbo.check_resultado
ADD fechaHora DATETIME;

