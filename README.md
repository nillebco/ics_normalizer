# ics_normalizer

Microsoft and Google do not interact well when it's matter of reading each other's ICS format.

In particular, events might be displayed in the wrong timezone (if the event organizers did not specify a timezone for the event and your timezone does not match the timezone of the organizer).

This API comes handy in these cases: deploy the project to your favourite provider, then
1. Get your Outlook Calendar's ICS (eg. https://outlook.office365.com/owa/calendar/xyz/reachcalendar.ics)
2. Visit Google Agenda
3. Create a new Calendar
4. Set as calendar address
   https://thisservice.yourprovider/calendar.ics?source=https://outlook.office365.com/owa/calendar/xyz/reachcalendar.ics

## developer

```sh
# install all the required deps
./cli init
# check your files (done on pre-commit hook if you installed it)
./cli lint
# run this service locally - http://localhost:8000/docs will expose the OpenAPI documentation
./cli serve
```
