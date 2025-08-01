CREATE TABLE watermark_tabel 
(
	last_load Varchar(2000)
)

SELECT min(Date_ID) FROM [dbo].[source_cars_data]

INSERT INTO [dbo].[watermark_tabel]
VALUES('DT00000') 
