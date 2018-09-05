var gulp = require('gulp')
var deploy = require('gulp-gh-pages')
var del = require('del')

var paths = {
  src: 'src/**/*',
  srcHTML: 'src/**/*.html',
  srcCSS: 'src/**/*.css',
  srcJS: 'src/**/*.js',
  tmp: 'tmp',
  tmpIndex: 'tmp/index.html',
  tmpCSS: 'tmp/**/*.css',
  tmpJS: 'tmp/**/*.js',
  dist: 'dist',
  distIndex: 'dist/index.html',
  distCSS: 'dist/**/*.css',
  distJS: 'dist/**/*.js'
};


/**
 * Push build to gh-pages
 */
gulp.task('deploy', function() {
  return gulp.src('./dist/**/*').pipe(deploy())
})