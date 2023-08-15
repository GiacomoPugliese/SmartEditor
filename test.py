import moviepy.editor as mp
main_video = mp.VideoFileClip('Giacomo Pugliese.mp4')
intro_video = mp.VideoFileClip('intro_li.mp4')
concatenated_video = mp.concatenate_videoclips([main_video, intro_video])
audio_clip = mp.AudioFileClip("intro_audio.mp3")
concatenated_video = concatenated_video.set_audio(audio_clip)
concatenated_video.write_videofile('test.mp4', codec='libx264')  # Use the same filename to overwrite it