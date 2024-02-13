# Install the latest odbc release from CRAN:
# install.packages("odbc")
# install.packages("RODBC")
# install.packages("tidyverse")

library(odbc)

# Set up connection to SQL Server. A pop up box will prompt you for your password
con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "SLCISDB19DW",
                 Database = "SLCoDW",
                 Trusted_Connection = "True",
                 UID = "mepstein",
                 PWD = rstudioapi::askForPassword("Database password"),
                 Port = 1433)


#############################################################

# JAIL DATA
# FILE: JRRP - JAIL DATA QUERY
# Files from John/JRRP - MARNI POWER BI QUERY FOR RECIDIVISM REPORT

jail <- dbGetQuery(con, "
      
      SELECT SYSID, PCP, COMDATE, RELDATE, LASTNAME, FIRSTNAM, REL_DESC, DEGREE
      FROM
      (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY SYSID ORDER BY DEG_RANK) AS CHG_RANK
      FROM
      (
      SELECT A.SYSID, A.PCP, A.COMDATE, A.RELDATE, A.LASTNAME, A.FIRSTNAM, 
      	CASE 
      		WHEN A.RELTYPE = 'IM' THEN 'IMG - REL TO IMMIGRATION'
      		ELSE C.EXTDESC END AS REL_DESC, B.DEGREE,
      	CASE
      		WHEN B.DEGREE = 'F1' THEN 1
      		WHEN B.DEGREE = 'F2' THEN 2
      		WHEN B.DEGREE = 'F3' THEN 3
      		WHEN B.DEGREE = 'MA' THEN 4
      		WHEN B.DEGREE = 'MB' THEN 5
      		WHEN B.DEGREE = 'MC' THEN 6
      		WHEN B.DEGREE = 'IN' THEN 7
      		ELSE 8 END AS DEG_RANK
      FROM [hs-cjs].BOOKING A
      LEFT OUTER JOIN [hs-cjs].CASE_CHARGE B
      ON A.SYSID = B.SYSID
      LEFT OUTER JOIN [hs-cjs].RTYPE C
      ON A.RELTYPE = C.ID
      WHERE A.JLOCAT = 'MAIN' AND NOT A.LASTNAME LIKE 'TEST %' AND (A.RELDATE >= '2022-04-01')
      ) A
      ) B
      WHERE B.CHG_RANK = 1
      order by PCP

    ")

##################################
# JRRP Smart sheet Data
# FILE: JRRP Smartsheet Query - Clean
##################################

jrrp <- dbGetQuery(con, "
  
  /****** Script for SelectTopNRows command from SSMS  ******/
  with jrr as (
  
  SELECT *
    FROM [hs-cjs].[JAIL_RESOURCE_DATA])
  
    select  [ID]
        ,[SO_NUMBER]
        ,[ENTRY_DATE]
        ,[CLIENT_NAME]
        ,[DATE_OF_BIRTH]
        ,[RELEASE_DATE]
  
        ,[WHERE_WILL_YOU_GO_WHEN_YOU_LEAVE_THE_JAIL]
        ,[WHAT_TYPE_OF_TRANSPORTATION_ARE_YOU_USING_TO_LEAVE]
        , (case when jrr.WAS_CLIENT_TRANSPORTED in ('false') then 0 else 1 end) as client_transport
        ,(case when jrr.provided_court_information in ('false') then 0 else 1 end) as court_info
        ,(case when jrr.[PROVIDED_JAIL_RELEASE_AGREEMENT_INFORMATION] in ('false') then 0 else 1 end) as jra
  	    ,ref_health = case when jrr.PROVIDED_REFERRALS_FOR like ('%ealthcar%') then 1 else 0 end
  	    ,ref_abuse = case when jrr.PROVIDED_REFERRALS_FOR like ('%violence%') then 1 else 0 end
  	    ,ref_transport = case when jrr.PROVIDED_REFERRALS_FOR like ('%ransportat%') then 1 else 0 end
        ,ref_employ = case when jrr.PROVIDED_REFERRALS_FOR like ('%mployme%') then 1 else 0 end
  	    ,ref_legal = case when jrr.PROVIDED_REFERRALS_FOR like ('%egal%') then 1 else 0 end
  	    ,ref_food = case when jrr.PROVIDED_REFERRALS_FOR like ('%food%') then 1 else 0 end 
  	    ,ref_mhsu = case when jrr.PROVIDED_REFERRALS_FOR like ('%ental%') then 1 else 0 end
        ,ref_house = case when jrr.PROVIDED_REFERRALS_FOR like ('%ousing%') then 1 else 0 end
  	    ,ref_educ = case when jrr.PROVIDED_REFERRALS_FOR like ('%ducatio%') then 1 else 0 end
        ,ref_specpop = case when jrr.PROVIDED_REFERRALS_FOR like ('%pecial%') then 1 else 0 end
  	    ,ref_other = case when jrr.PROVIDED_REFERRALS_FOR like ('%other%') then 1 else 0 end
  
        ,[PROVIDED_REFERRALS_FOR]
        ,(case when jrr.PROVIDED_SUPERVISION_INFORMATION in ('false') then 0 else 1 end) as sup_info
        ,(case when jrr.COORDINATED_REFERRAL_TO_MEDICAID_TAM in ('false') then 0 else 1 end) as medicaid
      
        ,[IMPORT_TIME] from jrr
      
")


##################################
# Jail demographic data - DOB, race, ethnicity, sex
# FILE: OMS - DEMOGRAPHICS QUERY - HS-CJS VIEWS
##################################

demog <- dbGetQuery(con, "
  
  -- OMS DEMOGRAPHICS QUERY FOR hs-cjs VIEWS
  
  SELECT A.*
  FROM
  (SELECT PCP, BIRTH AS DOB,
  CASE
  WHEN (RACE IN ('A','C','J') AND ETHNICITY IN ('AS','HS','IS','NH','OT'))
  OR (RACE IN ('A','C','J') AND (ETHNICITY = '' OR ETHNICITY IS NULL))
  OR (ETHNICITY IN ('AS','IS') AND (RACE = '' OR RACE IS NULL))
  OR (RACE = 'H' AND ETHNICITY IN ('AS','IS'))
  OR (RACE IN ('O','U','X') AND ETHNICITY IN ('AS','IS')) THEN 'Asian'
  WHEN (RACE = 'B' AND ETHNICITY IN ('HS','NH','OT'))
  OR (RACE = 'B' AND (ETHNICITY = '' OR ETHNICITY IS NULL)) THEN 'Black'
  WHEN (RACE = 'I' AND ETHNICITY IN ('AN','HS','NA','NH','OT'))
  OR (RACE = 'I' AND (ETHNICITY = '' OR ETHNICITY IS NULL))
  OR (RACE IN ('O','U','X') AND (ETHNICITY IN ('NA','AN')))
  OR (RACE = 'H' AND ETHNICITY IN ('NA','AN'))
  OR (ETHNICITY IN ('AN','NA') AND (RACE = '' OR RACE IS NULL)) THEN 'Native American'
  WHEN (RACE IN ('A','C','J','H','O','U','X') AND ETHNICITY = 'PI')
  OR (ETHNICITY = 'PI' AND (RACE = '' OR RACE IS NULL)) THEN 'Pacific Islander'
  WHEN (RACE IN ('A','C','J') AND ETHNICITY IN ('AN','NA','ME'))
  OR (RACE = 'B' AND ETHNICITY IN ('AN','AS','IS','ME','NA','PI'))
  OR (RACE = 'I' AND ETHNICITY IN ('AS','IS','ME','PI'))
  OR (RACE = 'W' AND ETHNICITY IN ('AN','AS','IS','NA','PI')) THEN 'Two or More'
  WHEN (RACE = 'W' AND ETHNICITY IN ('HS','ME','NH','OT'))
  OR (RACE = 'W' AND (ETHNICITY = '' OR ETHNICITY IS NULL))
  OR (RACE IN ('O','U','X','H') AND ETHNICITY = 'ME')
  OR (ETHNICITY = 'ME' AND (RACE = '' OR RACE IS NULL)) THEN 'White'
  ELSE 'Unknown' END AS RACE,
  CASE
  WHEN ETHNICITY = 'HS' THEN 'Hispanic'
  ELSE 'Non-Hispanic' END AS ETHNICITY,
  CASE
  WHEN SEX = 'M' THEN 'Male'
  WHEN SEX = 'F' THEN 'Female'
  ELSE 'Unknown' END AS SEX, ROW_NUMBER() OVER (PARTITION BY PCP ORDER BY COMDATE DESC) AS BOOK_RK
  FROM [hs-cjs].BOOKING
  WHERE JLOCAT = 'MAIN' AND NOT LASTNAME LIKE 'TEST %'
  AND (RELDATE >= '2022-01-01' OR RELDATE IS NULL)) A
  WHERE BOOK_RK = 1 AND NOT PCP IS NULL

")



###################################
# Charges
###################################


chg <- dbGetQuery(con, "
  
  
  WITH T1 AS (
  
  	SELECT A.SYSID, A.PCP, A.LASTNAME, A.FIRSTNAM, A.COMDATE, A.RELDATE, A.ADTYPE,
  	B.OFFENSE_CODE, B.OFFENSE_DESCRIPTION, B.DEGREE, B.CHARGE_ORDER, C.CASE_ORDER,
  	C.CASE_NUMBER, D.EXTDESC AS ADM_TYPE_DESC,
  	ROW_NUMBER() OVER (PARTITION BY A.SYSID ORDER BY C.CASE_ORDER, B.CHARGE_ORDER) AS CHG_RANK,
  	
  	E.NCIC_CODE AS CC_NCIC_CODE, E.NEW_NCIC,
  	G.NCIC_DESC AS CC_NCIC_DESC_MAP, F.NCIC_DESC AS MY_NCIC_DESC_MAP, 
  	G.NCIC_TYPE AS CC_NCIC_TYPE_MAP, F.NCIC_TYPE AS MY_NCIC_TYPE_MAP,
  	CASE
  		WHEN E.NEW_NCIC IN ('5401') THEN 'Hit and Run'
  		WHEN E.NEW_NCIC IN ('5403','5404') THEN 'DUI'
  		WHEN E.NEW_NCIC >= '5400' and E.NEW_NCIC <= '5499' THEN 'Traffic'
  		WHEN E.NEW_NCIC IN ('5707') THEN 'Trespassing'
  		WHEN E.NEW_NCIC >= '5700' and E.NEW_NCIC <= '5799' THEN 'Invasion of Privacy'
  		WHEN E.NEW_NCIC >= '3500' and E.NEW_NCIC <= '3599' THEN 'Drugs'
  		ELSE F.NCIC_TYPE END AS MY_NCIC_TYPE_MOD,
  	CASE
  		WHEN E.NEW_NCIC >= '900' and E.NEW_NCIC <= '999' THEN 'Homicide'
  		WHEN E.NEW_NCIC >= '1000' and E.NEW_NCIC <= '1099' THEN 'Kidnapping'
  		WHEN (E.NEW_NCIC >= '1100' and E.NEW_NCIC <= '1199') OR (E.NEW_NCIC >= '3600' and E.NEW_NCIC <= '3699') THEN 'Sex Offenses'
  		WHEN (E.NEW_NCIC >= '2000' and E.NEW_NCIC <= '2999') OR (E.NEW_NCIC >= '5100' and E.NEW_NCIC <= '5199') 
  				OR E.NEW_NCIC = '5707' THEN 'Property'
  		WHEN E.NEW_NCIC >= '5200' and E.NEW_NCIC <= '5299' THEN 'Weapons'
  		WHEN E.NEW_NCIC >= '3500' and E.NEW_NCIC <= '3599' THEN 'Drugs'
  		WHEN E.NEW_NCIC IN ('5403','5404') THEN 'DUI'
  		WHEN (E.NEW_NCIC >= '1200' and E.NEW_NCIC <= '1399') OR (E.NEW_NCIC >= '3800' and E.NEW_NCIC <= '3899')
  				OR (E.NEW_NCIC = '5016' AND E.OFFENSE_DESCRIPTION LIKE '%VIOLAT%PROTECT%') OR E.NEW_NCIC = '6411' 
  				OR E.OFFENSE_DESCRIPTION LIKE '%HUMAN TRAFFICKING%' THEN 'Person'
  		WHEN E.OFFENSE_CODE LIKE '77-29-5%' THEN 'Federal Detainee'
  		ELSE 'Other' END AS CRIME_TYPE 
  		
  
  FROM [hs-cjs].BOOKING A
  LEFT OUTER JOIN [hs-cjs].CASE_CHARGE B
  ON A.SYSID = B.SYSID
  LEFT OUTER JOIN [hs-cjs].CASE_MASTER C
  ON B.CASE_PK = C.CASE_PK
  LEFT OUTER JOIN [hs-cjs].MOAD D
  ON C.ADM_TYPE = D.ID
  
  LEFT OUTER JOIN [hs-cjs].NCIC_CODES E
  ON B.CHARGE_PK = E.CHARGE_PK and B.CASE_PK = E.CASE_PK
  LEFT OUTER JOIN [hs-cjs].NCIC_LOOKUP F
  ON E.NEW_NCIC = F.NCIC_CODE
  LEFT OUTER JOIN [hs-cjs].NCIC_LOOKUP G
  ON E.NCIC_CODE = G.NCIC_CODE
  
  WHERE A.JLOCAT = 'MAIN' AND NOT A.LASTNAME LIKE 'TEST %' AND A.RELDATE >= '2022-04-01')
  
  SELECT * 
  FROM T1

")



###################################
# Lifetime Bookings
###################################


lifebk <- dbGetQuery(con, "

  	SELECT SYSID, PCP, COMDATE, RELDATE, LASTNAME, FIRSTNAM, REL_DESC, DEGREE, DEG_RANK, daysInJail
	FROM
		(
		SELECT *, ROW_NUMBER() OVER (PARTITION BY SYSID ORDER BY DEG_RANK) AS CHG_RANK
		FROM
			(
			SELECT A.SYSID, A.PCP, A.COMDATE, A.RELDATE, A.LASTNAME, A.FIRSTNAM, 
				CASE 
					WHEN A.RELTYPE = 'IM' THEN 'IMG - REL TO IMMIGRATION'
					ELSE C.EXTDESC END AS REL_DESC, B.DEGREE,
				CASE
					WHEN B.DEGREE = 'F1' THEN 1
					WHEN B.DEGREE = 'F2' THEN 2
					WHEN B.DEGREE = 'F3' THEN 3
					WHEN B.DEGREE = 'MA' THEN 4
					WHEN B.DEGREE = 'MB' THEN 5
					WHEN B.DEGREE = 'MC' THEN 6
					WHEN B.DEGREE = 'IN' THEN 7
					ELSE 8 END AS DEG_RANK,
				DATEDIFF(day, A.COMDATE, A.RELDATE) AS daysInJail

			FROM [hs-cjs].BOOKING A
			LEFT OUTER JOIN [hs-cjs].CASE_CHARGE B
			ON A.SYSID = B.SYSID
			LEFT OUTER JOIN [hs-cjs].RTYPE C
			ON A.RELTYPE = C.ID
			WHERE A.JLOCAT = 'MAIN' AND NOT A.LASTNAME LIKE 'TEST %' --AND (A.RELDATE >= '2022-04-01')
			) A
		) B
	WHERE B.CHG_RANK = 1
	order by PCP, RELDATE
  


")

# Save file with the date
filename = paste0("Data/rawdata-", Sys.Date(), ".RData")
 
# Save as R environments
save(chg, demog, jail, jrrp, lifebk, file=filename)

### NOTE: the file used for the Capstone project is called Data/rawdata.RData

