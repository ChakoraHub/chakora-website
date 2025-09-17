import os
from kivy.uix.screenmanager import Screen
from kivy.graphics.texture import Texture
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label

class SyllabusLayout(BoxLayout):
    def load_syllabus(self, course_id):
        self.ids.welcome_label.text = f"Syllabus for Course ID: {course_id}"
        self.ids.syllabus_content.clear_widgets()
        self.ids.syllabus_content.add_widget(
            Label(
                text=f"📘 Details of Course {course_id}",
                size_hint_y=None,
                height=40
            )
        )

    def logout(self):
        self.ids.welcome_label.text = "You have logged out."
        self.ids.syllabus_content.clear_widgets()


class SyllabusApp(App):
    def build(self):
        # Kivy will auto-load syllabus.kv based on SyllabusLayout
        return SyllabusLayout()

if __name__ == "__main__":
    SyllabusApp().run()