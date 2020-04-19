#!/bin/bash

set -e

echo -e "\n\n####    Prozess gestartet: $(date '+%Y-%m-%d %H:%M:%S')    ################ \n#\n#"

startt=`date +%s%3N`

# Load envs

file_env() {
        local var="$1"
        local fileVar="${var}_FILE"
        local def="${2:-}"
        if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
                echo >&2 "error: both $var and $fileVar are set (but are exclusive)"
                exit 1
        fi
        local val="$def"
        if [ "${!var:-}" ]; then
                val="${!var}"
        elif [ "${!fileVar:-}" ]; then
                val="$(< "${!fileVar}")"
        fi
#       echo "$var geladen"
        export "$var"="$val"
        unset "$fileVar"
}

source ../netatmo_access_token.sh

##Read docker secrets.
secrets=(
        POSTGIS_DATABASE
        POSTGIS_USER
        POSTGIS_PASSWORD
       NETATMO_ACCESS_TOKEN
        NETATMO_REFRESH_TOKEN
        NETATMO_CLIENT_ID
        NETATMO_CLIENT_SECRET
        NETATMO_USERNAME
        NETATMO_PASSWORD
)

for e in "${secrets[@]}"; do
                file_env "$e"
done

##Ensure mandatory environment vars are set

envs=(
        POSTGIS_DATABASE
        POSTGIS_USER
        POSTGIS_PASSWORD
        NETATMO_ACCESS_TOKEN
        NETATMO_REFRESH_TOKEN
        NETATMO_CLIENT_ID
        NETATMO_CLIENT_SECRET
        NETATMO_USERNAME
        NETATMO_PASSWORD
        POSTGIS_HOSTNAME
)

for e in "${envs[@]}"; do
        if [ -z ${!e:-} ]; then
                echo "error: $e is not set"
                exit 1
        fi
done

OUTDIR=/netatmo_data
OUTFILE=netatmo_$(date +%Y%m%d_%H%M).json

HTTP_CODE=$(
curl -s -L -X GET \
  'http://api.netatmo.com/api/getpublicdata?access_token='$NETATMO_ACCESS_TOKEN'&lat_ne=49.518968&lon_ne=8.948021&lat_sw=49.300054&lon_sw=8.526077' \
  -H 'Accept: */*' \
  -H 'Accept-Encoding: gzip, deflate' \
  -H 'Connection: keep-alive' \
  -w"%{http_code}\n"\
  -o  $OUTDIR/$OUTFILE)

FILESIZE=$(wc -c <"$OUTDIR/$OUTFILE")
echo "Dateigroesse: $FILESIZE"

if [[ $HTTP_CODE == 200 ]] && [ $FILESIZE -ge 1000 ]; then

        echo "Datei $OUTFILE heruntergeladen."

        NETATMO_ACCESS_TOKEN=$(curl -s -X POST https://api.netatmo.net/oauth2/token -d 'grant_type=refresh_token&refresh_token='$NETATMO_REFRESH_TOKEN'&client_id='$NETATMO_CLIENT_ID'&client_secret='$NETATMO_CLIENT_SECRET'' | jq --raw-output '.access_token?')

        echo "Token erneuert: XXXXXX"${NETATMO_ACCESS_TOKEN: -4}
#       echo "Refresh Token:" $NETATMO_REFRESH_TOKEN

 elif [[ $HTTP_CODE == 403 ]]; then

        echo "ERROR. Server Antwort:"
        cat $OUTDIR/$OUTFILE
        echo "Versuche neuen Token zu erhalten"

        NETATMO_ACCESS_TOKEN=$(curl -s -X POST https://api.netatmo.net/oauth2/token -d 'grant_type=password&client_id='$NETATMO_CLIENT_ID'&client_secret='$NETATMO_CLIENT_SECRET'&username='$NETATMO_USERNAME'&password='$NETATMO_PASSWORD'' | jq --raw-output '.access_token?')
		echo "export NETATMO_ACCESS_TOKEN=$NETATMO_ACCESS_TOKEN" > ../netatmo_access_token.sh

        echo "Starte neuen Versuch mit neuem Token in 60s"
        echo "XXXXXX"${NETATMO_ACCESS_TOKEN: -4}
        sleep 60s
        bash $0 && exit

 else

        echo "ERROR: HTTP code:$HTTP_CODE. Verbindung pruefen"
        cat $OUTDIR/$OUTFILE
        exit 1
fi


PGPASSWORD=$POSTGIS_PASSWORD psql -p 5432 -h $POSTGIS_HOSTNAME -U $POSTGIS_USER -d $POSTGIS_DATABASE -c "\copy daten.json_input FROM $OUTDIR/$OUTFILE; SELECT daten.netatmo_all(); TRUNCATE TABLE daten.json_input;"

endt=`date +%s%3N`
runtime=$((endt-startt))
echo -e "#\n#\n####    Prozess beendet: $(date '+%Y-%m-%d %H:%M:%S'). Dauer: $runtime ms    #### \n\n"
