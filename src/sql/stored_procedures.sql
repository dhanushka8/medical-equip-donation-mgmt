CREATE DEFINER=`admin`@`%` PROCEDURE `update_hospital_coordinator`(
IN coordinatorId BIGINT,
IN firstName VARCHAR(45),
IN lastName VARCHAR(45),
IN phone VARCHAR(15),
IN email VARCHAR(15),
IN designation VARCHAR(45),
IN secondContactNumber VARCHAR(20),
IN salutation VARCHAR(5),
IN addressType VARCHAR(100),
IN adrName VARCHAR(45),
IN street VARCHAR(45),
IN city VARCHAR(45),
IN district VARCHAR(45),
IN state VARCHAR(45),
IN zipcode VARCHAR(45),
IN country VARCHAR(45))
BEGIN
	DECLARE v_adrId INT;
    
	UPDATE `covid_hospital_equipments`.`user` usr SET
		 usr.firstName = firstName, 
		 usr.lastName = lastName, 
         usr.contactNumber = phone,
		 usr.secondContactNumber = secondContactNumber,
		 usr.emailAddress = email,
		 usr.designation = designation,
		 usr.salutation = salutation
	WHERE usr.uid = coordinatorId;
    
    SELECT usr.addressId INTO v_adrId 
		FROM `covid_hospital_equipments`.`user` usr
		WHERE usr.uid = coordinatorId;
	
     UPDATE `covid_hospital_equipments`.`address` adr SET
		adr.addressType = addressType,
        adr.name = adrName, 
        adr.street = street, 
        adr.city = city, 
        adr.district = district, 
        adr.state = state, 
        adr.zipcode = zipcode, 
        adr.country = country
	WHERE adr.userId = coordinatorId;	 
	
END
