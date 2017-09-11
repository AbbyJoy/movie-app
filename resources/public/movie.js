

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

function makeSingleMovieItem(movie) {
    return "<li id='" + movie._id + "'>" + movie.name
        + " directed by " + movie.director
        + "<ul id='meta-" + movie._id + "'>"
        + "<li>Release Date: " + movie.releaseDate + "</li>"
        + "<li>Box Office Earnings: "
        + convertToCurrency(movie.boxOffice) + "</li>"
        + "<li>Stars: " + movie.stars + "</li>"
        + "<li>Reviews:</li>"
        + "</ul>"
        +"</li>";
};


function makeReviewItem(review) {
    return "<li id='" + review._id + "' class='" +review.movieId
        + "'>\"" + review.review + "\""
        + "<ul>"
        + "<li>Stars: " + review.rating + "</li>"
        + "<li>Reviewer: " + review.name + "</li>"
        + "<li> <button id='edit-" + review._id +"' class='edit-review-btn' type='button' name='editReviewBtn'>Edit</button></li>"
        + "</ul>"
        +"</li>";
};

// Get Movie list buton click event handler
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

// Get movie button click event handler
$("#movie-get-btn").click(function() {
    var movieId = $("#movie-entry-select").val();
    var urlMovie = "/movie/" + movieId;
    var movieMeta = "#meta-" + movieId;

    $.getJSON( urlMovie, function(data) {
        var reviews = [];
        $( "#movie-entry" ).html("<ul>" + makeSingleMovieItem(data) + "</ul>");
        $.each( data.reviews, function( key, review ) {
            reviews.push( makeReviewItem(review));
        });
        $( "<ul/>", {
            "class": "review-list",
            html: reviews.join( "" )
        }).appendTo( movieMeta );
    });
});

function makeReviewForm(review) {
    return "<div class='edit-review-form' id='" + review._id + "'>"
        + "<div><label for='name'>Name: </label>"
        + "<input type='text' name='name' value='" + review.name
        + "'/></div>"
        + "<div><label for='rating'>Stars: </label>"
        + "<input type='text' name='rating' value='" + review.rating
        + "'/></div>"
        + "<div><label for='review'>Review: </label>"
        + "<input name='review' value='" + review.review
        + "'/></div>"
        + "<div class='review-submit-btn' reviewid='" +review._id + "'>"
        + "<button type='submit'>Submit</button>"
        + "</div>"
        + "</div>";
};

$(document).on("click",".edit-review-btn", function(event) {
    var editBtnId = event.currentTarget.id;
    var reviewId = editBtnId.split("-")[1];
    var urlReview = "/review/" + reviewId;
    var reviewIdSelect = "#" + reviewId;

    $.getJSON( urlReview, function(data) {
        var reviewForm =  makeReviewForm(data);
        $(reviewIdSelect).html(reviewForm);
    });
});

$(document).on("click", ".review-submit-btn", function(event) {
    console.log("Clicked Submit Button");
    var reviewId = event.currentTarget.attributes.reviewid.nodeValue;
    var reviewFields = $('#' + reviewId).find('input');
    var urlReview = "/review/" + reviewId;
    var sendData = {};
    $.each(reviewFields, function(index, dataMap) {
        sendData[dataMap.name] = dataMap.value;
    });

    $.ajax({
        type: 'put',
	url: urlReview,
	data: sendData,
        sucess: function(){

        }
    });
    window.location.reload(true);
});

// Contains dropdown of movies for get movie
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
