--Queries used for Tableau

--1. Death rate globally
SELECT SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_cases)*100 as DeathRate
FROM PortfolioProject..covid_deaths$
WHERE continent is not null

--2. Total Death count by country
SELECT location, SUM(cast(new_deaths as int)) as total_death_count
FROM PortfolioProject..covid_deaths$
WHERE continent is null
and location not in ('World','European Union', 'International')
Group by location
ORDER by total_death_count DESC

--3. Infection Rate by country
SELECT Location, Population, MAX(total_cases) as HighestInfectionCount, MAX((total_cases/population))*100 as PercentPopulationInfected 
FROM PortfolioProject..covid_deaths$
GROUP by Location, Population
ORDER by PercentPopulationInfected desc

--4.
SELECT Location, Population, date, MAX(total_cases) as HighestInfectionCount, MAX((total_cases/population))*100 as PercentPopulationInfected 
FROM PortfolioProject..covid_deaths$
GROUP by Location, Population, date
ORDER by location 
