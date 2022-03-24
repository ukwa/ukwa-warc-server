WARC Server
-----------

The purpose of this simple service is to help route WARC file requests to the right file.

Notes
-----

This was used to generate the json index information

    warcio index -f offset,length,filename,warc-type,warc-target-uri test_warcs/wikipage.warc.gz

 424957+35730-1 = 460686


    curl -v -r 424957-460686 http://localhost:8008/by-filename/wikipage.warc.gz | gunzip -

 
