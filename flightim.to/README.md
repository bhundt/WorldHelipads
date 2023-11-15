A lot of helicopter landing sited in MSFS are not selectable but are actually landable. I wanted to have a better way to discover this sites and perform some helicopter centric flight planning. Thus I created a dataset which can be imported to LittleNavMap as userpoints. It contains all helipads which are currently knwon on OpenStreetMap and OpenAIP. Thanks to the OpenStreetMap contributors and OpenAIP and their contributors.

The dataset is split into three regions: 

- Region 1 contains North- and South-America
- Region 2 contains Europe and Africa
- Region 3 contains everything to the east

Please be aware: if you import all regions this will create more than 60.000 userpoints in LittleNavMap. The program handles this on my machine but operations like scrolling the list, deleting a lot of userpoints and so on can be a little slow. If you are unsure start with region 3 and see how the application behaves. USE AT YOUR OWN RISK and backup your LNM installation.

Installation: In LittleNavMap import the desired region(s) by selection Userpoints -> Import CSV ... If you get an error message when importing another region just restart LittleNavMap.

The script which creates this dataset can be found here: https://github.com/bhundt/WorldHelipads. If you find issues please use the github issue tracker. 

---
If you want to support such work you can buy me a coffee: https://www.buymeacoffee.com/bastianhundt