### Version History 

## 1.9.8

- Added option compatibility-mode to provide a workaround for when, using xml tables, the WriteToServerAsync method get stuck in a deadlock. When compatibility-mode set to true, the non-async method WriteToServer will be used. This does not suffer of the deadlock problem, but it also cannot be nicely cancelled and so exception management, and thus connection recovery, is more difficult.

## 1.9.7

- Quoted clustered key column names to avoid conflicting with reserved keywords
- Check for temporal tables only if running on a SQL engine version that supports that feature

## 1.9.6

- Exclude External Tables when generating table list
- Correctly manage StopIf SecondaryIndex flag

## 1.9.5

- Added support for synchronizing Identity values

## 1.9.4

- Added support for setting the command timeout.

## Earlier versions

Lost in time....or, dig into commits logs :)
