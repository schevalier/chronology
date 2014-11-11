module.exports = function(grunt) {
  require('load-grunt-tasks')(grunt);
  require('time-grunt')(grunt);

  grunt.initConfig({
    sass: {
      dist: {
        files: {
          'jia/static/css/style.css': 'jia/static/scss/style.scss'
        },
        options: {
          style: 'compressed'
        }
      }
    },
    watch: {
      css: {
        files: 'jia/static/scss/*.scss',
        tasks: ['sass']
      }
    },
    ngtemplates: {
      jia: {
        cwd: 'jia',
        src: 'static/app/**/*.html',
        dest: 'jia/static/build/js/partials.js',
        options: {
          concat: 'generated',
          prefix: '/'
        }
      }
    },
    copy: {
      templates: {
        files: [{
          expand: true,
          dot: true,
          cwd: 'jia/templates',
          dest: 'jia/templates/build',
          src: [
            'index.html'
          ]
        }]
      },
      images: {
        files: [{
          expand: true,
          dot: true,
          cwd: 'jia/static',
          dest: 'jia/static/build/img',
          src: [
            '**/img/**',
          ],
          flatten: true,
          filter: 'isFile'
        }]
      },
      fonts: {
        files: [{
          expand: true,
          dot: true,
          cwd: 'jia/static',
          dest: 'jia/static/build/fonts',
          src: [
            '**/fonts/**',
          ],
          flatten: true,
          filter: 'isFile'
        }]
      }
    },
    useminPrepare: {
      html: 'jia/templates/index.html',
      options: {
        root: 'jia',
        dest: 'jia',
        flow: {
          html: {
            steps:{ 
              js: ['concat'],
              css: ['concat']
            },
            post: {}
          }
        }
      }
    },
    usemin: {
      html: ['jia/templates/build/{,*/}*.html'],
      css: ['jia/static/build/{,*/}*.css'],
    },
  });

  grunt.registerTask('build', [
    'sass',
    'copy',
    'useminPrepare',
    'usemin',
    'ngtemplates',
    'concat'
  ]);
};
