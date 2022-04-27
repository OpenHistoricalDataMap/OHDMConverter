# osm2inter with extra table
Während der Importierung mit dem lua script werden die classcodes aus einer lua table gesetzt.


In einem sql script müssen nur noch die ids als richtige Primary Keys gesetzt werden. 
(Dies ist seitens osm2pgsql immer nötig!)

Das funktioniert auch, allerdings müssten dann in der Pflege der Datenbank 2 Tabellen kontrolliert werden.
1. Die classification Tabelle in der PostgreSQL Datenbank
2. Die table im lua Script