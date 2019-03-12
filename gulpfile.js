var gulp = require('gulp')
var deploy = require('gulp-gh-pages')
var del = require('del')
var inject = require('gulp-inject')
var webserver = require('gulp-webserver')
var htmlclean = require('gulp-htmlclean')
var cleanCSS = require('gulp-clean-css')
var sass = require('gulp-sass')
// var concat = require('gulp-concat')
// var uglify = require('gulp-uglify')

var paths = {
  src: 'src/**/*',
  srcHTML: 'src/**/*.html',
  srcCSS: 'src/**/*.css',
  srcJS: 'src/**/*.js',
  srcIMG: 'src/**/*.png',

  dist: 'dist',
  distIndex: 'dist/index.html',
  distCSS: 'dist/**/*.css',
  distJS: 'dist/**/*.js',
  distIMG: 'dist/images/'
}

gulp.task('default', ['watch'])

// checking for changes
gulp.task('watch', ['serve', 'sass'], function() {
  gulp.watch(paths.src, ['inject'])
  gulp.watch('./src/scss/**/*.scss', ['sass'])
})

gulp.task('serve', ['inject'], function() {
  return gulp.src(paths.dist).pipe(
    webserver({
      port: 3000,
      livereload: true
    })
  )
})

gulp.task('inject', ['copy'], function() {
  var css = gulp.src(paths.distCSS)
  var js = gulp.src(paths.distJS)
  return gulp
    .src(paths.distIndex)
    .pipe(inject(css, { relative: true }))
    .pipe(inject(js, { relative: true }))
    .pipe(gulp.dest(paths.dist))
})

/**
 * Push build to gh-pages
 */
gulp.task('deploy', ['inject'], function() {
  return gulp.src('./dist/**/*').pipe(deploy())
})

// copy all HTML files from the src directory to the dist directory
gulp.task('html', function() {
  return gulp.src(paths.srcHTML).pipe(gulp.dest(paths.dist))
})

// copy all CSS files from the src directory dto the dist directory
gulp.task('css', function() {
  return gulp.src(paths.srcCSS).pipe(gulp.dest(paths.dist))
})

// copy all JS files from the src directory to the dist directory
gulp.task('js', function() {
  return gulp.src(paths.srcJS).pipe(gulp.dest(paths.dist))
})
// copy all image files from the src directory to the dist directory
gulp.task('img', function() {
  return gulp.src(paths.srcIMG).pipe(gulp.dest(paths.dist))
})

// combine tasks
gulp.task('copy', ['html', 'css', 'js', 'img'])

// build tasks
gulp.task('build', ['inject:dist'])

gulp.task('inject:dist', ['copy:dist'], function() {
  var css = gulp.src(paths.distCSS)
  var js = gulp.src(paths.distJS)
  return gulp
    .src(paths.distIndex)
    .pipe(inject(css, { relative: true }))
    .pipe(inject(js, { relative: true }))
    .pipe(gulp.dest(paths.dist))
})

gulp.task('html:dist', function() {
  return gulp
    .src(paths.srcHTML)
    .pipe(htmlclean())
    .pipe(gulp.dest(paths.dist))
})
gulp.task('css:dist', function() {
  return (
    gulp
      .src(paths.srcCSS)
      // .pipe(concat('style.min.css'))
      .pipe(cleanCSS())
      .pipe(gulp.dest(paths.dist))
  )
})
gulp.task('js:dist', function() {
  return (
    gulp
      .src(paths.srcJS)
      // .pipe(concat('script.min.js'))
      // .pipe(uglify())
      .pipe(gulp.dest(paths.dist))
  )
})

gulp.task('copy:dist', ['html:dist', 'css:dist', 'js:dist'])

gulp.task('clean', function() {
  del([paths.dist])
})

// compile the Sass files
gulp.task('sass', function() {
  return gulp
    .src('./src/scss/**/*.scss')
    .pipe(sass().on('error', sass.logError))
    .pipe(gulp.dest('./src/css'))
    .pipe(gulp.dest('./dist/css'))
})