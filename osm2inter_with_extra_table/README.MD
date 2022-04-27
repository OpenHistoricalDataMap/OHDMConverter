# osm2inter with extra table
Bei dieser Konvertierungsvariante wird anhand der mapfeatures Klassen eine weitere Tabelle erstellt.<br>
Die Idee hierbei ist, nach der Konvertierung die intermediate Datenbank upzudaten.

Die osm_id der nodes|ways|relations Tabelle wird der Referenztabelle verknüpft und <br>
anhand der mapfeatures werden die classcodes gesetzt

## Problem
Es liegt keine Lösungsmöglichkeit für die lange Laufzeit vor.

> Komplette Konvertierung mit berlin.osm: ca. 7 Mio nodes, 1 Mio ways, 18000 relations nach 4h  abgebrochen