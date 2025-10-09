# Incautaciones Marihuana


### Crear la base de datos
```JavaScript
mongorestore --uri "mongodb+srv://<usuario>:<password>@<cluster>.mongodb.net" --db IncautacionesMarihuana './dump/'
```

### Diagrama Logico

```mermaid
erDiagram
    Incautacion {
        _id ObjectId PK
        fecha DATETIME
        cantidadEnKg DECIMAL
        municipio_id ObjectId FK
    }

    Municipio {
        _id ObjectId PK
        nombre VARCHAR(60)
        codigoMunicipio INT
        departamento_id ObjectId FK
    }

    Departamento {
        _id ObjectId PK
        nombre VARCHAR(60)
        codigoDepartamento INT
    }

    Municipio }|--|| Departamento: ""
    Incautacion }|--|| Municipio: ""
```