use IncautacionesMarihuana;

// 1.

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

// 2.

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

// 3.

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