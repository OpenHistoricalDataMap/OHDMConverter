-- Exctract inter.classifiaction to the standard output
-- nessarry to save mapfeatures as csv file
-- NOTE: this script must run with access rights on the database
-- author: SteSad
COPY inter.classification TO STDOUT WITH DELIMITER ',';