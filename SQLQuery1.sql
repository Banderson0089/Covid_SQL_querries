Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, 
	SUM(cast (new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
FROM PortfolioProject..covid_deaths$
--Where location like '%states%'
where continent is not null
--Group by date
order by 1,2


--looking at total population vs Vaccinations
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
Sum(cast(vac.new_vaccinations as int)) OVER (Partition by dea.location order by dea.location, dea.date) as rollingPeopleVac
, (rollingPeopleVac/population)*100
From PortfolioProject..covid_deaths$ dea
Join PortfolioProject..covid_Vaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null
	order by 2,3

--USE CTE
With PopvsVac (continent, Location, Date, Population, new_vaccinations, rollingPeopleVac)
	as
	(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
Sum(cast(vac.new_vaccinations as int)) OVER (Partition by dea.location order by dea.location, dea.date) as rollingPeopleVac
--, (rollingPeopleVac/population)*100
From PortfolioProject..covid_deaths$ dea
Join PortfolioProject..covid_Vaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null
	)
Select *, (rollingPeopleVac/population)*100
From PopvsVac 

--Temp table
Drop table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar (255),
Location nvarchar (255),
Date datetime,
Population numeric,
New_vaccinatiosn numeric,
rollingPeopleVac numeric
)

Insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
Sum(cast(vac.new_vaccinations as int)) OVER (Partition by dea.location order by dea.location, dea.date) as rollingPeopleVac
--, (rollingPeopleVac/population)*100
From PortfolioProject..covid_deaths$ dea
Join PortfolioProject..covid_Vaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null
	
Select *, (rollingPeopleVac/population)*100
From #PercentPopulationVaccinated


--Creating view for visualization
Create View PercentPopulationVaccinated2 as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
Sum(cast(vac.new_vaccinations as int)) OVER (Partition by dea.location order by dea.location, dea.date) as rollingPeopleVac
From PortfolioProject..covid_deaths$ dea
Join PortfolioProject..covid_Vaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null

Select * FROM PercentPopulationVaccinated