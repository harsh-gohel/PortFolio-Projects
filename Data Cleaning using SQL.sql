-- Dataset name: NashvilleHousing.xlsx



-- Here, we will do data cleaning using SQL.
''' Performed Operations:
1.standardizing the date column.
2.Populate the Property Address Data.
3.Breaking the address into separate columns(StreetAddress, state).
4.making YES and NO to Y and N respectivly in "SoldAsVacant" Column.
5.Removing Duplicates.
6.Deleting unused columns.



'''
--using PortfolioProject as default
use PortfolioProject;

--having a glance at description of NashvilleHousing table.
EXEC sp_help 'NashvilleHousing';

--have a glance at our NashvilleHousing table data.

select * 
from NashvilleHousing;

-- 1. standardizing the date column.


-- Adding a new column SaleDateConverted.
ALTER TABLE	NashvilleHousing
Add SaleDateConverted Date;

--updating the SaleDateConverted column.
update NashvilleHousing
SET SaleDateConverted = CONVERT(date, SaleDate);

--verifying the update operation.

select SaleDateConverted, Saledate
From NashvilleHousing;

-- Droping the 'SaleDate' column.
ALTER table NashvilleHousing
DROP COLUMN SaleDate;


--2. Populate the Property Address Data.

SELECT PropertyAddress
FROM NashvilleHousing 
WHERE PropertyAddress is NULL;

-- Here, I have used self join operation where a new column will be displayed if the 'propertyaddress' is null.

SELECT a.ParcelID, a.PropertyAddress,b.ParcelID, b.PropertyAddress,
ISNULL(a.PropertyAddress, b.PropertyAddress) 
FROM NashvilleHousing a
join NashvilleHousing b 
on a.ParcelID = b.ParcelID AND a.[UniqueID ] <> b.[UniqueID ]
where a.PropertyAddress is null;

-- Using above logic , we will perform the update operation.

UPDATE a
SET PropertyAddress = ISNULL(a.PropertyAddress, b.PropertyAddress) 
FROM NashvilleHousing a
join NashvilleHousing b 
on a.ParcelID = b.ParcelID AND a.[UniqueID ] <> b.[UniqueID ]
where a.PropertyAddress is null;

--verifying the update operation by checking the 'PropertyAddress' Column.

SELECT PropertyAddress
FROM NashvilleHousing 
WHERE PropertyAddress is NULL;


-- 3. Breaking the address into separate columns(StreetAddress, state).

SELECT PropertyAddress
FROM NashvilleHousing;

SELECT PropertyAddress,
SUBSTRING(PropertyAddress,1,CHARINDEX(',', PropertyAddress)-1) as Address,
SUBSTRING(PropertyAddress,CHARINDEX(',', PropertyAddress) + 1, LEN(PropertyAddress)) as City
FROM NashvilleHousing;

--creating 2 new columns, 'PropertyStreetAddress' and 'Propertycity'.

ALTER TABLE NashvilleHousing
ADD PropertyStreetAddress nvarchar(255);

ALTER TABLE NashvilleHousing
ADD PropertyCity nvarchar(255);

-- Updating the 'PropertyStreetAddress' and 'Propertycity' Columns.
UPDATE NashvilleHousing
SET PropertyStreetAddress = SUBSTRING(PropertyAddress,1,CHARINDEX(',', PropertyAddress)-1);

UPDATE NashvilleHousing
SET PropertyCity = SUBSTRING(PropertyAddress,CHARINDEX(',', PropertyAddress) + 1, LEN(PropertyAddress));


SELECT PropertyAddress, PropertyStreetAddress, PropertyCity FROM
NashvilleHousing;


-- We will also make separate columns of 'OwnerAddress'.

Alter table NashvilleHousing
Add OwnerStreetAdress nvarchar(255);

Alter table NashvilleHousing
Add OwnerCity nvarchar(255);

Alter table NashvilleHousing
Add OwnerState nvarchar(255);

UPDATE NashvilleHousing
SET OwnerStreetAdress = PARSENAME(REPLACE(OwnerAddress,',','.'),3),
OwnerCity = PARSENAME(REPLACE(OwnerAddress,',','.'),2),
OwnerState = PARSENAME(REPLACE(OwnerAddress,',','.'),1);

SELECT OwnerAddress, OwnerStreetAdress, OwnerCity, OwnerState 
FROM NashvilleHousing;

-- 4. making YES and NO to Y and N respectivly in 'SoldAsVacant' Column.

SELECT distinct(SoldAsVacant)
FROM NashvilleHousing;

UPDATE NashvilleHousing
SET SoldAsVacant = 'Y' WHERE SoldAsVacant = 'Yes';

UPDATE NashvilleHousing
SET SoldAsVacant = 'N' WHERE SoldAsVacant = 'No';


-- 5. Removing Duplicates.


-- fetching all duplicate records. Here We have used window function ROW_NUMBER() inside a subquery
select * from
	(select *, 
		ROW_NUMBER() OVER(
		PARTITION BY ParcelId,
		PropertyAddress,
		SalePrice,
		SaleDateConverted,
		LegalReference 
			ORDER BY 
				UniqueID) rn
	FROM NashvilleHousing)
	as x
	where x.rn > 1;

--deleting duplicate data using Common Table Expressions


WITH RowNumCTE AS
	(select *, 
		ROW_NUMBER() OVER(
		PARTITION BY ParcelId,
		PropertyAddress,
		SalePrice,
		SaleDateConverted,
		LegalReference 
			ORDER BY 
				UniqueID) rn
	FROM NashvilleHousing)
	DELETE RowNumCTE From NashvilleHousing
	WHERE rn > 1;


--6. Deleting unused columns.

SELECT * FROM NashvilleHousing;

ALTER TABLE NashvilleHousing
DROP COLUMN OwnerAddress, PropertyAddress, TaxDistrict;
