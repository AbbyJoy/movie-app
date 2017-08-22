
//TODO: Add the dropdown/button for get movie

var convertToCurrency = function(number){
    return '$' +  number.replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1,");
};

// Takes movie map as parameter
function makeMovieItem(movie) {
    return "<li id='" + movie._id + "'>" + movie.name
        + " directed by " + movie.director
        + "<ul>"
        + "<li>Release Date: " + movie.releaseDate + "</li>"
        + "<li>Box Office Earnings: "
        + convertToCurrency(movie.boxOffice) + "</li>"
        + "<li>Stars: " + movie.stars + "</li>"
        + "</ul>"
        +"</li>";
};

$("#movie-list-btn").click(function() {
    $.getJSON( "/movie", function(data) {
        var items = [];

        // Call back function takes each movie object and formats
        // it to a list item which contains another list of
        // the movie's attributes.
        $.each( data, function( key, movie ) {
            items.push( makeMovieItem(movie));
        });

        $( "<ul/>", {
            "class": "full-movie-list",
            html: items.join( "" )
        }).appendTo( "#movie-list" );
    });
});

$("#movie-get-btn").click(function() {
    var movieId = $("#movie-entry-select").val();
    var urlMovie = "/movie/" + movieId;

    $.getJSON( urlMovie, function(data) {
        $( "#movie-entry" ).html("<ul>" + makeMovieItem(data) + "</ul>");
    });
});

$(document).ready( function () {
    $.getJSON( "/movie", function(data) {
        var items = [];

        // Call back function takes each movie object and formats
        // it to a list item which contains another list of
        // the movie's attributes.
        $.each( data, function( key, movie ) {
            items.push( "<option value='" + movie._id + "'>" + movie.name
                        +"</option>" );
        });

        $("#movie-entry-select").append(items.join(""));
    });

});
