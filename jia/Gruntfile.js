module.exports = function(grunt) {
  require('load-grunt-tasks')(grunt);
  require('time-grunt')(grunt);

  grunt.initConfig({
    ngtemplates: {
      jia: {
        cwd: 'jia',
        src: 'static/partials/**/*.html',
        dest: 'jia/static/build/js/partials.js',
        options: {
          usemin: 'jia/static/build/js/jia.js',
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
    'ngtemplates',
    'copy',
    'useminPrepare',
    'concat',
    'usemin'
  ]);

};
