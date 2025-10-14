// Debes haber importado tu base de datos a tu cluster que permita replication

// Comandos desde la terminal de mongosh:

const session = db.getMongo().startSession();

const IncauDB = session.getDatabase("IncautacionesMarihuana");

session.startTransaction();
