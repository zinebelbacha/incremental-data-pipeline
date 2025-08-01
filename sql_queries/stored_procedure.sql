CREATE PROCEDURE UpdateWatermarkTable
	@lastload VARCHAR(200)
AS 
BEGIN 
	-- Start the transaction 
	BEGIN TRANSACTION;

	-- Update the incremental column in the table 
	UPDATE watermark_table
	SET last_load = @lastload

END;
