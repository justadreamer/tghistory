DB=postgresdb

# testing DB:
pg_ctl status -D $DB
status=$?


# starting DB if needed
if [ $status -ne 0 ] 
then
	echo "starting pg"
	pg_ctl stop -D $DB 
	pg_ctl start -D $DB
fi


# extracting chat history into DB
pipenv run python extract_tghistory.py

# running analysis
R -e "rmarkdown::render('tghistory_analytics.Rmd')"


# stopping DB
if [ $status -ne 0 ] 
then
	echo "stopping pg"
	pg_ctl stop -D $DB 
fi
