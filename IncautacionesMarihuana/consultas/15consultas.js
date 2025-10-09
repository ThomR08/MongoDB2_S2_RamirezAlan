use IncautacionesMarihuana;

// 1.

db.Municipio.aggregate([{
    $match: {
        nombre: {
            $regex: "^La",
            $options: "i"
        }
    }
}, {
    $lookup: {
        from: "Incautacion",
        localField: "_id",
        foreignField: "municipio_id",
        as: "inca"
    }
},{
    $project: {
        _id: 0,
        nombreMunicipio: "$nombre",
        codigoMunicipio: 1,
        cantidadIncautada: {
            $sum: "$inca.cantidadEnKg"
        }
    }
},{
    $sort: {
        cantidadIncautada: -1
    }
}]);