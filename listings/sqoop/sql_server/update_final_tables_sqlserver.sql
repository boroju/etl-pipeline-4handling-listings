USE [MLSLoad]
GO

SET ansi_nulls ON
GO
SET quoted_identifier ON
GO
-- =============================================
-- Author:		Randall Gonzalez
-- Create date: 2-2-2020
-- Description:	Updates MLS Listings final tables with data sqooped from HDFS.
-- =============================================
CREATE OR ALTER PROCEDURE [dbo].[MLS_Listings_UpdateFinalTables_sp]
AS
BEGIN

	SET NOCOUNT ON;

	BEGIN TRY
	
		/****
		Listings start
		****/
	
		DROP TABLE IF EXISTS [dbo].[Listing_hadoop_tmp]
		DROP TABLE IF EXISTS [dbo].[Listing_hadoop_tmp2]
		DROP TABLE IF EXISTS [MLS].[dbo].[Listing_hadoop_new]		
		
		-- For Sqoop table we need an index
		DROP INDEX IF EXISTS [IDX_Listings_sqoop] ON [dbo].[Listings_sqoop];
		CREATE UNIQUE NONCLUSTERED INDEX [IDX_Listings_sqoop] ON [dbo].[Listings_sqoop] ([mls] ASC, [mls_listing_id] ASC) ON [PRIMARY];
		
		-- Note: opening transaction and commitng after each section to avoid transaction log full issues 
		BEGIN TRANSACTION
			SELECT s.[mls] AS [MLS]
				, s.[mls_listing_id] AS [MLSListingID]
				, s.[street_address] AS [StreetAddress]
				, s.[unit_type] AS [UnitType]
				, s.[unit] AS [Unit]
				, s.[city] AS [City]
				, s.[state] AS [State]
				, s.[zip] AS [Zip]
				, s.[latitude] AS [Latitude]
				, s.[longitude] AS [Longitude]
				, s.[legal_description] AS [LegalDescription]
				, s.[subdivision] AS [Subdivision]
				, s.[lot] AS [Lot]
				, s.[block] AS [Block]
				, s.[legal_tract] AS [LegalTract]
				, s.[book] AS [Book]
				, s.[section] AS [Section]
				, s.[township] AS [Township]
				, s.[range] AS [Range]
				, s.[apn] AS [APN]
				, s.[county_name] AS [CountyName]
				, s.[fips] AS [FIPS]
				, s.[census_tract_geo_id] AS [CensusTractGeoID]
				, s.[school_district] AS [SchoolDistrict]
				, s.[property_type] AS [PropertyType]
				, s.[property_sub_type] AS [PropertySubType]
				, s.[property_description] AS [PropertyDescription]
				, s.[lot_size_acres] AS [LotSizeAcres]
				, CAST(s.[lot_size_sq_ft] AS int) AS [LotSizeSqFt]
				, s.[zoning] AS [Zoning]
				, s.[restrictions] AS [Restrictions]
				, s.[easements] AS [Easements]
				, s.[water_source] AS [WaterSource]
				, s.[septic_sewer] AS [SepticSewer]
				, s.[sfha] AS [SFHA]
				, s.[gated_community] AS [GatedCommunity]
				, s.[hoa] AS [HOA]
				, s.[hoa_name] AS [HOAName]
				, s.[hoa_management_co] AS [HOAManagementCo]
				, s.[hoa_management_co_phone] AS [HOAManagementCoPhone]
				, s.[occupant_type] AS [OccupantType]
				, s.[ownership_type] AS [OwnershipType]
				, s.[owner_type] AS [OwnerType]
				, s.[owner_name] AS [OwnerName]
				, s.[owner_phone] AS [OwnerPhone]
				, s.[year_built] AS [YearBuilt]
				, s.[year_updated] AS [YearUpdated]
				, s.[number_of_units] AS [NumberOfUnits]
				, CAST(s.[living_area_sq_ft] as int) AS [LivingAreaSqFt]
				, s.[living_area_sq_ft_source] AS [LivingAreaSqFtSource]
				, s.[building_style] AS [BuildingStyle]
				, s.[stories] AS [Stories]
				, s.[beds] AS [Beds]
				, s.[full_baths] AS [FullBaths]
				, s.[half_baths] AS [HalfBaths]
				, s.[basement] AS [Basement]
				, s.[finished_basement_pct] AS [FinishedBasementPct]
				, s.[garage_type] AS [GarageType]
				, s.[garage_style] AS [GarageStyle]
				, CAST(s.[garage_spaces] AS int) AS [GarageSpaces]
				, s.[roof_type] AS [RoofType]
				, s.[exterior_material] AS [ExteriorMaterial]
				, s.[foundation] AS [Foundation]
				, s.[pool] AS [Pool]
				, s.[condition] AS [Condition]
				, s.[property_tax_appraisal] AS [PropertyTaxAppraisal]
				, s.[property_tax] AS [PropertyTax]
				, s.[property_tax_year] AS [PropertyTaxYear]
				, s.[hoa_dues] AS [HOADues]
				, s.[hoa_dues_frequency] AS [HOADuesFrequency]
				, s.[hoa_dues_description] AS [HOADuesDescription]
				, CASE s.[rent_sale]
					WHEN 'Rental' THEN 'R'
					WHEN 'Sale' THEN 'S'
				  END AS [RentSale]
				, s.[entry_date] AS [EntryDate]
				, s.[listing_date] AS [ListingDate]
				, s.[listing_status] AS [ListingStatus]
				, s.[listing_status_detail] AS [ListingStatusDetail]
				, s.[status_date] AS [StatusDate]
				, s.[current_price] AS [CurrentPrice]
				, s.[current_price_as_of_date] AS [CurrentPriceAsOfDate]
				, s.[orig_price] AS [OrigPrice]
				, s.[orig_listing_date] AS [OrigListingDate]
				, s.[contract_date] AS [ContractDate]
				, s.[closed_price] AS [ClosedPrice]
				, s.[closed_date] AS [ClosedDate]
				, s.[days_on_market] AS [DaysOnMarket]
				, CAST(s.[dom_date] AS date) AS [DOMDate]
				, s.[cumulative_days_on_market] AS [CumulativeDaysOnMarket]
				, s.[sale_circumstances] AS [SaleCircumstances]
				, s.[listing_conditions] AS [ListingConditions]
				, s.[listing_url] AS [ListingURL]
				, s.[listing_image_url] AS [ListingImageURL]
				, s.[listing_image_url_count] AS [ListingImageURLCount]
				, s.[listing_image_url_date] AS [ListingImageURLDate]
				, s.[loan_amount] AS [LoanAmount]
				, s.[public_remarks] AS [PublicRemarks]
				, s.[realtor_remarks] AS [RealtorRemarks]
				, s.[listing_broker_name] AS [ListingBrokerName]
				, s.[listing_broker_id] AS [ListingBrokerID]
				, s.[listing_agent_name] AS [ListingAgentName]
				, s.[listing_agent_id] AS [ListingAgentID]
				, s.[listing_agent_phone] AS [ListingAgentPhone]
				, s.[listing_agent_email] AS [ListingAgentEmail]
				, s.[brokerage_name] AS [BrokerageName]
				, s.[brokerage_phone] AS [BrokeragePhone]
				, s.[selling_agent_name] AS [SellingAgentName]
				, s.[selling_agent_id] AS [SellingAgentID]
				, s.[commissions] AS [Commissions]
				, s.[buyer_agent_name] AS [BuyerAgentName]
				, s.[buyer_agent_id] AS [BuyerAgentID]
				, s.[buyer_commission_pct] AS [BuyerCommissionPct]
				, s.[street_address_raw] AS [StreetAddressRaw]
				, s.[city_raw] AS [CityRaw]
				, s.[state_raw] AS [StateRaw]
				, s.[zip_raw] AS [ZipRaw]
				, CAST(NULL AS datetime2) AS [AddressCleanedTimeStamp]
				, o1.[ailPropertyID] AS [ailPropertyID]
				, s.[source] AS [ailSource]
				, s.[source_reference] AS [ailSourceReference]
				, s.[source_listing_id] AS [ailSourceListingID]
				, CAST(s.[source_as_of_date] AS datetime2) AS [ailSourceTimeStamp]
				, CAST(s.[create_timestamp] AS datetime2) AS [CreateTimeStamp]
				, CAST(s.[update_timestamp] AS datetime2) AS [UpdateTimeStamp]
				-- Surrogate keys
				, o1.[ListingID] AS [ListingID]
				, o2.[ListingID] AS [ListingID_Legacy]
			INTO [dbo].[Listing_hadoop_tmp]
			FROM [dbo].[Listings_sqoop] s
			LEFT OUTER JOIN [MLS].[dbo].[Listing_hadoop] o1
			ON s.mls = o1.mls and s.mls_listing_id = o1.MLSListingID
			LEFT OUTER JOIN [MLS].[dbo].[Listing_dt] o2
			ON s.mls = o2.mls and s.mls_listing_id = o2.MLSListingID			
		COMMIT TRANSACTION
		
		-- Create index on dbo.Listing_hadoop_tmp.ListingID column
		DROP INDEX IF EXISTS [IDX_Listing_hadoop_tmp] ON [dbo].[Listing_hadoop_tmp];
		CREATE NONCLUSTERED INDEX [IDX_Listing_hadoop_tmp] ON [dbo].[Listing_hadoop_tmp] ([ListingID] ASC) ON [PRIMARY];
		
		-- Create _tmp2 table with extra identity column
		SELECT * INTO [dbo].[Listing_hadoop_tmp2] FROM [MLS].[dbo].[Listing_hadoop] WHERE 1 = 0;
		ALTER TABLE [dbo].[Listing_hadoop_tmp2] ADD ListingID_Tmp BIGINT NOT NULL IDENTITY(1,1);
		ALTER TABLE [dbo].[Listing_hadoop_tmp2] ALTER COLUMN ListingID BIGINT NULL;
		
		-- Enable identity insert on _tmp2 table
		SET IDENTITY_INSERT [dbo].[Listing_hadoop_tmp2] ON;
		
		BEGIN TRANSACTION
			-- I need to list all the columns when inserting into an identity column, otherwise SQL Server throws error
			INSERT INTO [dbo].[Listing_hadoop_tmp2] WITH (TABLOCK)
			(MLS, MLSListingID, StreetAddress, UnitType, Unit, City, State, Zip, Latitude, Longitude, LegalDescription, Subdivision, Lot, Block, LegalTract, Book, Section, Township, Range, APN, CountyName, FIPS, CensusTractGeoID, SchoolDistrict, PropertyType, PropertySubType, PropertyDescription, LotSizeAcres, LotSizeSqFt, Zoning, Restrictions, Easements, WaterSource, SepticSewer, SFHA, GatedCommunity, HOA, HOAName, HOAManagementCo, HOAManagementCoPhone, OccupantType, OwnershipType, OwnerType, OwnerName, OwnerPhone, YearBuilt, YearUpdated, NumberOfUnits, LivingAreaSqFt, LivingAreaSqFtSource, BuildingStyle, Stories, Beds, FullBaths, HalfBaths, Basement, FinishedBasementPct, GarageType, GarageStyle, GarageSpaces, RoofType, ExteriorMaterial, Foundation, Pool, Condition, PropertyTaxAppraisal, PropertyTax, PropertyTaxYear, HOADues, HOADuesFrequency, HOADuesDescription, RentSale, EntryDate, ListingDate, ListingStatus, ListingStatusDetail, StatusDate, CurrentPrice, CurrentPriceAsOfDate, OrigPrice, OrigListingDate, ContractDate, ClosedPrice, ClosedDate, DaysOnMarket, DOMDate, CumulativeDaysOnMarket, SaleCircumstances, ListingConditions, ListingURL, ListingImageURL, ListingImageURLCount, ListingImageURLDate, LoanAmount, PublicRemarks, RealtorRemarks, ListingBrokerName, ListingBrokerID, ListingAgentName, ListingAgentID, ListingAgentPhone, ListingAgentEmail, BrokerageName, BrokeragePhone, SellingAgentName, SellingAgentID, Commissions, BuyerAgentName, BuyerAgentID, BuyerCommissionPct, StreetAddressRaw, CityRaw, StateRaw, ZipRaw, AddressCleanedTimeStamp, ailPropertyID, ailSource, ailSourceReference, ailSourceListingID, ailSourceTimeStamp, CreateTimeStamp, UpdateTimeStamp, ListingID, ListingID_Legacy, ListingID_Tmp)
			SELECT 
			MLS, MLSListingID, StreetAddress, UnitType, Unit, City, State, Zip, Latitude, Longitude, LegalDescription, Subdivision, Lot, Block, LegalTract, Book, Section, Township, Range, APN, CountyName, FIPS, CensusTractGeoID, SchoolDistrict, PropertyType, PropertySubType, PropertyDescription, LotSizeAcres, LotSizeSqFt, Zoning, Restrictions, Easements, WaterSource, SepticSewer, SFHA, GatedCommunity, HOA, HOAName, HOAManagementCo, HOAManagementCoPhone, OccupantType, OwnershipType, OwnerType, OwnerName, OwnerPhone, YearBuilt, YearUpdated, NumberOfUnits, LivingAreaSqFt, LivingAreaSqFtSource, BuildingStyle, Stories, Beds, FullBaths, HalfBaths, Basement, FinishedBasementPct, GarageType, GarageStyle, GarageSpaces, RoofType, ExteriorMaterial, Foundation, Pool, Condition, PropertyTaxAppraisal, PropertyTax, PropertyTaxYear, HOADues, HOADuesFrequency, HOADuesDescription, RentSale, EntryDate, ListingDate, ListingStatus, ListingStatusDetail, StatusDate, CurrentPrice, CurrentPriceAsOfDate, OrigPrice, OrigListingDate, ContractDate, ClosedPrice, ClosedDate, DaysOnMarket, DOMDate, CumulativeDaysOnMarket, SaleCircumstances, ListingConditions, ListingURL, ListingImageURL, ListingImageURLCount, ListingImageURLDate, LoanAmount, PublicRemarks, RealtorRemarks, ListingBrokerName, ListingBrokerID, ListingAgentName, ListingAgentID, ListingAgentPhone, ListingAgentEmail, BrokerageName, BrokeragePhone, SellingAgentName, SellingAgentID, Commissions, BuyerAgentName, BuyerAgentID, BuyerCommissionPct, StreetAddressRaw, CityRaw, StateRaw, ZipRaw, AddressCleanedTimeStamp, ailPropertyID, ailSource, ailSourceReference, ailSourceListingID, ailSourceTimeStamp, CreateTimeStamp, UpdateTimeStamp, ListingID, ListingID_Legacy,
			ListingID AS ListingID_Tmp
			FROM [dbo].[Listing_hadoop_tmp] t
			WHERE t.ListingID IS NOT NULL --> need index here 
		COMMIT TRANSACTION
		
		-- Disable identity insert on _tmp2 table 
		SET IDENTITY_INSERT [dbo].[Listing_hadoop_tmp2] OFF;	
		
		BEGIN TRANSACTION
			INSERT INTO [dbo].[Listing_hadoop_tmp2] WITH (TABLOCK)
			SELECT t.* -- ListingID_Tmp value will be added as identity
			FROM [dbo].[Listing_hadoop_tmp] t
			WHERE t.ListingID IS NULL --> need index here 
		COMMIT TRANSACTION	

		-- Getting rid of tables as soon as we no longer need them
		DROP TABLE IF EXISTS [dbo].[Listing_hadoop_tmp];		
		
		-- Creating new table, using this approach instead of SELECT INTO to prevent identity from being carried over
		SELECT * INTO [MLS].[dbo].[Listing_hadoop_new] FROM [MLS].[dbo].[Listing_hadoop] WHERE 1 = 0
		
		BEGIN TRANSACTION		
			INSERT INTO [MLS].[dbo].[Listing_hadoop_new] WITH (TABLOCK)
			SELECT t.[MLS]
				, t.[MLSListingID]
				, t.[StreetAddress]
				, t.[UnitType]
				, t.[Unit]
				, t.[City]
				, t.[State]
				, t.[Zip]
				, t.[Latitude]
				, t.[Longitude]
				, t.[LegalDescription]
				, t.[Subdivision]
				, t.[Lot]
				, t.[Block]
				, t.[LegalTract]
				, t.[Book]
				, t.[Section]
				, t.[Township]
				, t.[Range]
				, t.[APN]
				, t.[CountyName]
				, t.[FIPS]
				, t.[CensusTractGeoID]
				, t.[SchoolDistrict]
				, t.[PropertyType]
				, t.[PropertySubType]
				, t.[PropertyDescription]
				, t.[LotSizeAcres]
				, t.[LotSizeSqFt]
				, t.[Zoning]
				, t.[Restrictions]
				, t.[Easements]
				, t.[WaterSource]
				, t.[SepticSewer]
				, t.[SFHA]
				, t.[GatedCommunity]
				, t.[HOA]
				, t.[HOAName]
				, t.[HOAManagementCo]
				, t.[HOAManagementCoPhone]
				, t.[OccupantType]
				, t.[OwnershipType]
				, t.[OwnerType]
				, t.[OwnerName]
				, t.[OwnerPhone]
				, t.[YearBuilt]
				, t.[YearUpdated]
				, t.[NumberOfUnits]
				, t.[LivingAreaSqFt]
				, t.[LivingAreaSqFtSource]
				, t.[BuildingStyle]
				, t.[Stories]
				, t.[Beds]
				, t.[FullBaths]
				, t.[HalfBaths]
				, t.[Basement]
				, t.[FinishedBasementPct]
				, t.[GarageType]
				, t.[GarageStyle]
				, t.[GarageSpaces]
				, t.[RoofType]
				, t.[ExteriorMaterial]
				, t.[Foundation]
				, t.[Pool]
				, t.[Condition]
				, t.[PropertyTaxAppraisal]
				, t.[PropertyTax]
				, t.[PropertyTaxYear]
				, t.[HOADues]
				, t.[HOADuesFrequency]
				, t.[HOADuesDescription]
				, t.[RentSale]
				, t.[EntryDate]
				, t.[ListingDate]
				, t.[ListingStatus]
				, t.[ListingStatusDetail]
				, t.[StatusDate]
				, t.[CurrentPrice]
				, t.[CurrentPriceAsOfDate]
				, t.[OrigPrice]
				, t.[OrigListingDate]
				, t.[ContractDate]
				, t.[ClosedPrice]
				, t.[ClosedDate]
				, t.[DaysOnMarket]
				, t.[DOMDate]
				, t.[CumulativeDaysOnMarket]
				, t.[SaleCircumstances]
				, t.[ListingConditions]
				, t.[ListingURL]
				, t.[ListingImageURL]
				, t.[ListingImageURLCount]				
				, t.[ListingImageURLDate]
				, t.[LoanAmount]
				, t.[PublicRemarks]
				, t.[RealtorRemarks]
				, t.[ListingBrokerName]
				, t.[ListingBrokerID]
				, t.[ListingAgentName]
				, t.[ListingAgentID]
				, t.[ListingAgentPhone]
				, t.[ListingAgentEmail]
				, t.[BrokerageName]
				, t.[BrokeragePhone]
				, t.[SellingAgentName]
				, t.[SellingAgentID]
				, t.[Commissions]
				, t.[BuyerAgentName]
				, t.[BuyerAgentID]
				, t.[BuyerCommissionPct]
				, t.[StreetAddressRaw]
				, t.[CityRaw]
				, t.[StateRaw]
				, t.[ZipRaw]
				, t.[AddressCleanedTimeStamp]
				, t.[ailPropertyID]
				, t.[ailSource]
				, t.[ailSourceReference]
				, t.[ailSourceListingID]
				, t.[ailSourceTimeStamp]
				, t.[CreateTimeStamp]
				, t.[UpdateTimeStamp]
				, t.ListingID_Tmp AS [ListingID] 
				, t.[ListingID_Legacy]
			FROM [dbo].[Listing_hadoop_tmp2] t
		COMMIT TRANSACTION
		
		-- Getting rid of tables as soon as we no longer need them
		DROP TABLE IF EXISTS [dbo].[Listing_hadoop_tmp2];

		-- Backup final tables before doing any rename
		DROP TABLE IF EXISTS [MLS].[dbo].[Listing_hadoop_old];
		EXEC MLS..sp_rename 'dbo.Listing_hadoop.PK_Listing_hadoop', 'PK_Listing_hadoop_old', 'INDEX';
		EXEC MLS..sp_rename 'dbo.Listing_hadoop', 'Listing_hadoop_old';
		EXEC MLS..sp_rename 'dbo.Listing_hadoop_new', 'Listing_hadoop';

		-- Apply page compression and PK
		ALTER TABLE [MLS].[dbo].[Listing_hadoop] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);
		ALTER TABLE [MLS].[dbo].[Listing_hadoop] ADD CONSTRAINT [PK_Listing_hadoop] PRIMARY KEY CLUSTERED ([MLS] ASC, [MLSListingID] ASC) 
		ON [PRIMARY];
		
		-- Drop leftover tables
		DROP TABLE IF EXISTS [MLS].[dbo].[Listing_hadoop_old];
		DROP TABLE IF EXISTS [MLS].[dbo].[Listing_hadoop_new]
		DROP INDEX IF EXISTS [IDX_Listings_sqoop] ON [dbo].[Listings_sqoop];
		
		/****
		Listings end
		****/
		
	END TRY
	BEGIN CATCH
		
		-- Get error details
		DECLARE @ErrorMessage varchar(max) = ERROR_MESSAGE()
		DECLARE @ErrorSeverity int = ERROR_SEVERITY()
		DECLARE @ErrorState smallint = ERROR_STATE()

		-- Transaction uncommittable
		IF (XACT_STATE()) = -1
			ROLLBACK TRANSACTION;

		-- Transaction committable
		IF (XACT_STATE()) = 1
			COMMIT TRANSACTION;

		-- Send error back to Stonebranch
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState)	

	END CATCH

END
