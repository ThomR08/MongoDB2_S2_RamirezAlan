// Debes haber importado tu base de datos a tu cluster que permita replication

// Comandos desde la terminal de mongosh:

const session = db.getMongo().startSession();

const IncauDB = session.getDatabase("IncautacionesMarihuana");

// Ejercicios básicos

// 1. Encuentra todos los municipios que empiezan por “San”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "^san",
            $options: "i"
        }
    }
}]);

// 2. Lista los municipios que terminan en “ito”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "ito$",
            $options: "i"
        }
    }
}]);

// 3. Busca los municipios cuyo nombre contenga la palabra “Valle”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "Valle",
            $options: "i"
        }
    }
}]);

// 4. Devuelve los municipios cuyo nombre empiece por vocal.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "^[aeiou]",
            $options: "i"
        }
    }
}]);

// 5. Filtra los municipios que terminen en “al” o “el”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "(al|el)$",
            $options: "i"
        }
    }
}]);

// Ejercicios intermedios

// 6. Encuentra los municipios cuyo nombre contenga dos vocales seguidas.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "[aeiou]{2}",
            $options: "i"
        }
    }
}]);

// 7. Obtén todos los municipios con nombres que contengan la letra “z”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "z",
            $options: "i"
        }
    }
}]);

// 8. Lista los municipios que empiecen con “Santa” y tengan cualquier cosa después.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "^Santa",
            $options: "i"
        }
    }
}]);

// 9. Encuentra municipios cuyo nombre tenga exactamente 6 letras.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "^.{6}$",
            $options: "i"
        }
    }
}]);

// 10. Filtra los municipios cuyo nombre tenga 2 palabras.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /^\S+ \S+$/i
        }
    }
}]);

// Ejercicios avanzados

// 11. Encuentra municipios cuyos nombres terminen en “ito” o “ita”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /(ito|ita)$/i
        }
    }
}]);

// 12. Lista los municipios que contengan la sílaba “gua” en cualquier posición.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /gua/i
        }
    }
}]);

// 13. Devuelve los municipios que empiecen por “Puerto” y terminen en “o”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /^Puerto.*o$/i
        }
    }
}]);

// 14. Encuentra municipios con nombres que tengan más de 10 caracteres.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /.{10}/i,
        }
    }
}]);

// 15. Busca municipios que no contengan vocales.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /^(?!.*[aeiou])/i,
        }
    }
}]);

// Ejercicios aplicados a pipelines

// 16. Muestra la cantidad total incautada en municipios que empiezan con “La”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /^la/i,
        }
    }
},
{
    $lookup: {
        from: "Incautacion",
        localField: "_id",
        foreignField: "municipio_id",
        as: "inca"
    }
},
{
    $project: {
        _id: 0,
        nombreMunicipio: "$nombre",
        codigoMunicipio: 1,
        cantidadIncautada: {
            $sum: "$inca.cantidadEnKg"
        }
    }
},
{
    $sort: {
        cantidadIncautada: -1
    }
}]);

// 17. Calcula el total de incautaciones en municipios cuyo nombre termine en “co”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /co$/i,
        }
    }
},
{
    $lookup: {
        from: "Incautacion",
        localField: "_id",
        foreignField: "municipio_id",
        as: "inca"
    }
},
{
    $project: {
        _id: 0,
        nombreMunicipio: "$nombre",
        codigoMunicipio: 1,
        cantidadIncautada: {
            $sum: "$inca.cantidadEnKg"
        }
    }
},
{
    $sort: {
        cantidadIncautada: -1
    }
}]);

// 18. Obtén el top 5 de municipios con más incautaciones cuyo nombre contenga la letra “y”.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /y/i,
        }
    }
},
{
    $lookup: {
        from: "Incautacion",
        localField: "_id",
        foreignField: "municipio_id",
        as: "inca"
    }
},
{
    $project: {
        _id: 0,
        nombreMunicipio: "$nombre",
        codigoMunicipio: 1,
        cantidadIncautada: {
            $sum: "$inca.cantidadEnKg"
        }
    }
},
{
    $sort: {
        cantidadIncautada: -1
    }
},
{
    $limit: 5
}]);

// 19. Encuentra los municipios que empiecen por “San” y agrupa la cantidad incautada por año.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /^san/i,
        }
    }
},
{
    $lookup: {
        from: "Incautacion",
        localField: "_id",
        foreignField: "municipio_id",
        as: "inca"
    }
},
{
    $unwind: "$inca"
},
{
    $group: {
        _id: {
            ano: {
                $year: "$inca.fecha"
            },
            municipioId: "$_id"
        },
        nombreMunicipio: {
            $first: "$nombre"
        },
        totalIncaKg: {
            $sum: "$inca.cantidadEnKg"
        }
    }
},
{
    $sort: {
        "_id.ano": -1,
        totalIncaKg: -1
    }
},
{
    $project: {
        _id: 0,
        nombreMunicipio: 1,
        codigoMunicipio: 1,
        ano: "$_id.ano",
        totalIncaKg: 1
    }
}]);

// 20. Lista los departamentos que tengan al menos un municipio cuyo nombre termine en “ito” o “ita”, y muestra la cantidad total incautada en ellos.

IncauDB.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: /(ito|ita)$/i,
        }
    }
},
{
    $lookup: {
        from: "Incautacion",
        localField: "_id",
        foreignField: "municipio_id",
        as: "inca"
    }
},
{
    $unwind: "$inca"
},
{
    $group: {
        _id: "$departamento_id",
        totalIncaKg: {
            $sum: "$inca.cantidadEnKg"
        }
    }
},
{
    $sort: {
        totalIncaKg: -1
    }
},
{
    $lookup: {
        from: "Departamento",
        localField: "_id",
        foreignField: "_id",
        as: "depa"
    }
},
{
    $unwind: "$depa"
},
{
    $project: {
        _id: 0,
        nombreDepartamento: "$depa.nombre",
        totalIncaKgEnMunisItoIta: 1
    }
}]);