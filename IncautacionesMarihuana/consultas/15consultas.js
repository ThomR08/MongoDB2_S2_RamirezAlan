use IncautacionesMarihuana;

// 1. ¿Cuántos municipios comienzan con "La" y cuál es la cantidad total incautada en ellos?

db.Municipio.aggregate([{
    $match: {
        nombre: {
            $regex: "^La",
            $options: "i"
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

// 2. Top 5 departamentos donde los municipios terminan en "al" y la cantidad incautada.

db.Municipio.aggregate([{
    $match: {
        nombre: {
            $regex: "al$",
            $options: "i"
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

// 3. Por cada año, muestra los 3 municipios con más incautaciones, pero únicamente si su nombre contiene la letra "z".

db.Municipio.aggregate([{
    $match: {
        nombre: {
            $regex: "z",
            $options: "i"
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
        Municipio: {
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
        Municipio: "$nombre",
        codigoMunicipio: "$codigoMunicipio",
        ano: "$_id.ano",
        totalIncaKg: "$totalIncaAno"
    }
}])

// 4. ¿Qué unidad de medida aparece en registros de municipios que empiecen por "Santa"?

// El ejercicio no aplica para este sistema de información, todas las incautaciones están normalizadas a kilogramos

// 5. ¿Cuál es la cantidad promedio de incautaciones en los municipios cuyo nombre contiene "Valle"?

db.Municipio.aggregate([{
    $match: {
        nombre: {
            $regex: "Valle",
            $options: "i"
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
        promedioIncautacion: {
            $avg: "$inca.cantidadEnKg"
        }
    }
},
{
    $sort: {
        promedioIncautacion: -1
    }
}]);

// 6. ¿Cuántos registros hay en municipios cuyo nombre contenga exactamente 7 letras?

db.Municipio.aggregate([{
    $match: {
        "nombre": {
            $regex: "^.{7}$",
            $options: "i"
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
        _id: "$inca.municipio_id",
        cantidadIncautaciones: {
            $sum: 1
        },
        nombreMunicipio: "$nombre",
        codigoMunicipio: 1
    }
},
{
    $project: {
        _id: 0,
        nombreMunicipio: 1,
        codigoMunicipio: 1,
        cantidadIncautaciones: 1
    }
}]);

// 7. ¿Cuáles son los 10 municipios con mayor cantidad incautada en 2020?

db.Incautacion.aggregate([{
    $group: {
        _id: {
            municipio_id: "$municipio_id",
            ano: {
                $year: "$fecha"
            }
        },
        totalIncaKg: {
            $sum: "$cantidadEnKg"
        }
    }
},{
    $match: {
        "_id.ano": 2020
    }
},{
    $sort: {
        totalIncaKg: -1
    }
},{
    $limit: 10
},{
    $lookup: {
        from: "Municipio",
        localField: "_id.municipio_id",
        foreignField: "_id",
        as: "Muni"
    }
},{
    $unwind: "$Muni"
},{
    $project: {
        _id: 0,
        nombreMunicipio: "$Muni.nombre",
        totalIncaKg: 1
    }
}])

// 8. Por cada departamento, muestra el municipio con más cantidad incautada.