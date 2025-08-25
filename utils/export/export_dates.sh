#!/bin/bash

echo "Exporting stopgap data between:"
echo "    $1"
echo "    $2"
end_time=($2)

declare -a tables=(posts comments front_pages subreddit_pages)
declare -a strip_tables=(front_pages subreddit_pages)

for table in "${tables[@]}"
do
	echo "Exporting: $table"
	mysqldump \
		--no-create-info \
		--insert-ignore \
		--skip-extended-insert \
		civilservant_production \
		$table \
		--where "created_at >= \"${1}\" and created_at <= \"${2}\"" \
		> "${end_time[0]}-${table}.sql"
done

echo "Stripping primary keys"

for table in "${strip_tables[@]}"
do
	echo "Stripping: $table"
	python3 strip_export_key.py "${end_time[0]}-${table}.sql"
done
echo "Done"
