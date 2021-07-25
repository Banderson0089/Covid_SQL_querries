--Standardize data format
SELECT SaleDate, Sale_date
FROM PortfolioProject..NashvilleHousing


ALTER TABLE NashvilleHousing
Add Sale_Date DATE;

UPDATE NashvilleHousing
SET Sale_Date = CONVERT(Date,SaleDate)



--Populate Property Address data
SELECT *
FROM PortfolioProject..NashvilleHousing
WHERE PropertyAddress is null


SELECT *
FROM PortfolioProject..NashvilleHousing
ORDER by ParcelID

SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress, ISNULL(a.PropertyAddress, b.PropertyAddress)
FROM PortfolioProject..NashvilleHousing a
JOIN PortfolioProject..NashvilleHousing b
	on a.ParcelID = b.ParcelID
	AND a.[UniqueID ] <> b.[UniqueID ]
	WHERE a.PropertyAddress is null

UPDATE a
SET a.PropertyAddress = ISNULL(a.PropertyAddress, b.PropertyAddress)
FROM PortfolioProject..NashvilleHousing a
JOIN PortfolioProject..NashvilleHousing b
	on a.ParcelID = b.ParcelID
	AND a.[UniqueID ] <> b.[UniqueID ]
	WHERE a.PropertyAddress is null


--Owner address to address, city, state

SELECT PropertyAddress
FROM PortfolioProject..NashvilleHousing
ORDER by ParcelID

SELECT 
SUBSTRING(PropertyAddress, 1, CHARINDEX(',',PropertyAddress)-1) as address,
SUBSTRING(PropertyAddress, (CHARINDEX(',',PropertyAddress) +1),LEN(PropertyAddress))as city

FROM PortfolioProject..NashvilleHousing
ORDER by ParcelID

ALTER TABLE NashvilleHousing
Add PropertyAddressSplit Nvarchar(255)

ALTER TABLE NashvilleHousing
Add City Nvarchar(255)

UPDATE NashvilleHousing
SET PropertyAddressSplit = SUBSTRING(PropertyAddress, 1, CHARINDEX(',',PropertyAddress)-1)

UPDATE NashvilleHousing
SET City= SUBSTRING(PropertyAddress, (CHARINDEX(',',PropertyAddress) +1),LEN(PropertyAddress))

SELECT PropertyAddressSplit, City, OwnerAddress
FROM NashvilleHousing

ALTER TABLE NashvilleHousing
Add OwnerAddressSplit Nvarchar(255)

ALTER TABLE NashvilleHousing
Add OwnerCity Nvarchar(255)

ALTER TABLE NashvilleHousing
Add OwnerST Nvarchar(255)


SELECT 
PARSENAME (REPLACE (OwnerAddress, ',', '.'),3),
PARSENAME (REPLACE (OwnerAddress, ',', '.'),2),
PARSENAME (REPLACE (OwnerAddress, ',', '.'),1)
FROM NashvilleHousing

UPDATE NashvilleHousing
SET OwnerAddressSplit = PARSENAME (REPLACE (OwnerAddress, ',', '.'),3)

UPDATE NashvilleHousing
SET OwnerCity = PARSENAME (REPLACE (OwnerAddress, ',', '.'),2)

UPDATE NashvilleHousing
SET OwnerST = PARSENAME (REPLACE (OwnerAddress, ',', '.'),1)

SELECT PropertyAddressSplit, City, OwnerAddressSplit, OwnerCity, OwnerST
FROM NashvilleHousing


--Change y/n to yes/no "Sold as Vacant"
SELECT DISTINCT(SoldAsVacant), count(SoldAsVacant)
FROM NashvilleHousing
GROUP by SoldAsVacant

SELECT SoldAsVacant
, CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
	WHEN SoldAsVacant = 'N' THEN 'NO'
	ELSE SoldAsVacant
	END
FROM NashvilleHousing

UPDATE NashvilleHousing
SET SoldAsVacant = CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
	WHEN SoldAsVacant = 'N' THEN 'NO'
	ELSE SoldAsVacant
	END


--Remove duplicates
WITH RowNumCTE as(
SELECT*,
ROW_NUMBER() OVER (
PARTITION by ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
ORDER by UniqueID) row_num
FROM NashvilleHousing
)
DELETE
FROM RowNumCTE
WHERE row_num> 1


WITH RowNumCTE as(
SELECT*,
ROW_NUMBER() OVER (
PARTITION by ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
ORDER by UniqueID) row_num
FROM NashvilleHousing
)
SELECT *
FROM RowNumCTE
WHERE row_num> 1

--delete unused columns


ALTER TABLE PortfolioProject.dbo.NashvilleHousing
DROP COLUMN OwnerAddress, PropertyAddress, OwnerState, SaleDate

SELECT * 
FROM PortfolioProject.dbo.NashvilleHousing